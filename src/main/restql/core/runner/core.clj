(ns restql.core.runner.core
  (:require [restql.log :as log]
            [clojure.set :as s]
            [restql.core.query :as query]
            [restql.core.runner.executor :as executor]
            [restql.core.statement.core :as statement])
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue TimeUnit]))

(defn- all-done? [state]
  (and (empty? (:to-do state)) (empty? (:requested state))))

(defn- can-request?
  "given a single query item and the map with the current results
   returns true if all the dependencies of the query-item are
   resolved"
  [query-item state]
  (let [deps  (query/get-dependencies query-item)
        dones (->> state :done (map first) (into #{}))]
    (empty? (s/difference deps dones))))

(defn- all-that-can-request
  "takes a state with queries :done :requested and :to-do and returns
   a sequence of pairs with only the queries that can be executed, because all
   their dependencies are already met.

   Example return: ([:cart {:with ...}] [:freight {:with ...})"
  [state]
  (filter #(can-request? % state) (:to-do state)))

(defn- is-done? [[query-item-key _] state]
  (->> state
       :done
       (map first)
       (into #{})
       query-item-key
       nil?
       not))

(defn- update-state
  "it passes all to-do queries that could be requested to :requested state and
   adds a completed request to the :done state"
  [state completed]
  {:done (conj (:done state) completed)
   :requested (filter
               #(and (not= (first completed) (first %)) (not (is-done? % state)))
               (into (:requested state) (all-that-can-request state)))
   :to-do (filter #(not (can-request? % state)) (:to-do state))})

(defn- exception-result [e]
  {:type "exception" :message (.getMessage e) :exception e})

(defn- do-run
  "it separates all queries in three states, :done :requested and :to-do
   then dispatches all to-dos, changing their statuses to :requested.
   As the results arrive in the results queue, update the query status to :done and dispatch all to-dos again.
   When all queries are :done, the process is complete, and the :done part of the state is returned.
   If the deadline is reached returns {:error :timeout}, if a request fails with an exception returns it."
  [query ^BlockingQueue results deadline dispatch!]
  (loop [state {:done [] :requested [] :to-do query}]
    (if (all-done? state)
      (:done state)
      (do
        (doseq [to-do (all-that-can-request state)]
          (dispatch! {:to-do to-do :state state}))
        (let [remaining (max 0 (- deadline (System/currentTimeMillis)))
              completed (.poll results remaining TimeUnit/MILLISECONDS)]
          (cond
            (nil? completed) {:error :timeout}
            (= ::exception (first completed)) (exception-result (second completed))
            :else (recur (update-state state completed))))))))

; ######################################; ######################################

(defn- log-if-408-or-aborted [result uid resource]
  (let [status (:status result)]
    (cond
      (= status 408) (log/warn {:session uid :resource resource} "Request timed out")
      (nil? status)  (log/warn {:session uid :resource resource} "Request aborted")
      :else          :no-action)))

(defn- log-status [result uid resource]
  "in case of result being a list, for multiplexed calls"
  (if (sequential? result)
    (doall (map #(log-if-408-or-aborted % uid resource) (flatten result)))
    (log-if-408-or-aborted result uid resource)))

(defn- generate-uuid! []
  (.toString (java.util.UUID/randomUUID)))

(defn- build-and-execute [context {:keys [to-do state]} query-opts uuid]
  (let [[query-name statement] to-do
        from (:from (second statement))
        result (->
                (statement/build context statement (:done state))
                (as-> requests (executor/do-request context requests query-opts))
                (deref))]
    (log-status result uuid from)
    (vector query-name result)))

(defn- make-request
  "performs the requests of a query item in a separate thread,
   putting the result in the results queue"
  [context ^BlockingQueue results query-opts uuid next-req]
  (future
    (.put results (try
                    (build-and-execute context next-req query-opts uuid)
                    (catch Throwable e
                      [::exception e])))))

; ######################################; ######################################

(defn run
  "returns a future with the :done list of [query-name result] pairs,
   {:error :timeout} if timeout-ms is reached or an exception map"
  [context query {:keys [_debugging] :as query-opts} timeout-ms]
  (let [results  (LinkedBlockingQueue.)
        deadline (+ (System/currentTimeMillis) timeout-ms)
        uuid     (generate-uuid!)]
    (future
      (do-run query results deadline #(make-request context results query-opts uuid %)))))
