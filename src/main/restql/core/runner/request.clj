(ns restql.core.runner.request
  (:require [clj-http.client :as http]
            [clj-http.conn-mgr :as conn-mgr]
            [clojure.tools.logging :as log]
            [restql.parser.json :as json]
            [ring.util.codec :refer [form-encode]]
            [environ.core :refer [env]]
            [restql.hooks.core :as hook]
            [restql.core.runner.debugging :as debugging])
  (:import [java.net URLDecoder SocketTimeoutException]
           [java.util.concurrent ExecutionException TimeoutException]
           [org.apache.http.conn ConnectTimeoutException ConnectionPoolTimeoutException]))

(def default-values {:pool-connections-per-host 500
                     :pool-total-connections 10000
                     :connection-keep-alive-seconds 10
                     :connection-timeout 2000})

(defn- get-default
  ([key] (if (contains? env key) (read-string (env key)) (default-values key)))
  ([key default] (if (contains? env key) (read-string (env key)) default)))

(defonce client-connection-pool
  (conn-mgr/make-reusable-conn-manager {:timeout           (get-default :connection-keep-alive-seconds)
                                        :threads           (get-default :pool-total-connections)
                                        :default-per-route (get-default :pool-connections-per-host)}))

(defn- fmap [f m]
  (reduce-kv #(assoc %1 %2 (f %3)) {} m))

(defn- decode-url [string]
  (try
    (URLDecoder/decode string "utf-8")
    (catch Exception e
      string)))

(defn- parse-query-params
  "this function takes a request object (with :url and :query-params)
  and transforms query params that are sets into vectors"
  [request]
  (update-in request [:query-params]
             #(fmap (fn [query-param-value]
                      (if (or (sequential? query-param-value) (set? query-param-value))
                        (->> query-param-value (map decode-url) (into []))
                        (decode-url query-param-value))) %)))

(defn- get-forward-params [query-opts]
  (-> query-opts
      (some-> :forward-params)
      (as-> forward-params (if (nil? forward-params) {} forward-params))))

(defn- get-query-params [request]
  (-> request
      (some-> :query-params)
      (as-> query-params (if (nil? query-params) {} query-params))))

(defn- valid-query-params [request query-opts]
  (->> (get-query-params request)
       (merge (get-forward-params query-opts))
       (filter (fn [[_ v]] (some? v)))
       (into {})))

(defn- convert-response [{:keys [status body headers]} {:keys [_debugging metadata response-time url params timeout resource]}]
  (let [parsed (if (string? body) body (slurp body))
        base {:status        status
              :headers       headers
              :url           url
              :metadata      metadata
              :timeout       timeout
              :params        params
              :resource      resource
              :response-time response-time}]
    (try
      (assoc base
             :body (if (= parsed "")
                     parsed
                     (json/parse-string parsed)))
      (catch Exception e
        (log/error {:status status
                    :headers headers
                    :url url
                    :params params
                    :resource resource
                    :response-time response-time
                    :message (.getMessage e)}
                   "error parsing response")
        (assoc base
               :parse-error true
               :body parsed)))))

; ConnectionPoolTimeoutException extends ConnectTimeoutException, so it must be checked first
(defn- get-error-status [exception]
  (cond
    (instance? java.lang.IllegalArgumentException exception) 400
    (instance? clojure.lang.ExceptionInfo exception) 408
    (instance? TimeoutException exception) 408
    (instance? ConnectionPoolTimeoutException exception) 0
    (instance? ConnectTimeoutException exception) 408
    (instance? SocketTimeoutException exception) 408
    :else 0))

(defn- get-error-message [exception]
  (cond
    (instance? clojure.lang.ExceptionInfo exception) (str "Error: " (.getMessage exception))
    (instance? java.lang.IllegalArgumentException exception) (str "Error: " (.getMessage exception))
    (instance? TimeoutException exception) "RequestTimeoutException"
    (instance? ConnectionPoolTimeoutException exception) "PoolTimeoutException"
    (instance? ConnectTimeoutException exception) "ConnectionTimeoutException"
    (instance? SocketTimeoutException exception) "ReadTimeoutException"
    :else "Internal error"))

(defn- get-after-ctx [{:keys [context ctx status response-time request result]}]
  (merge {} ctx request result context {:status status
                                        :response-time response-time}))

(defn- response-with-debug [response request-map query-opts]
  (into response (debugging/build-map response request-map query-opts)))

(defn- request-respond-callback [result & {:keys [request
                                                  request-timeout
                                                  time-before
                                                  query-opts
                                                  request-map
                                                  before-hook-ctx
                                                  context]}]
  (let [log-data {:resource (:from request)
                  :timeout  request-timeout
                  :success  true}]
    (log/debug (assoc log-data :success true
                      :status (:status result)
                      :time (- (System/currentTimeMillis) time-before))
               "Request successful")
    (let [response (convert-response result {:debugging (:debugging query-opts)
                                             :metadata  (:metadata request)
                                             :resource  (:from request)
                                             :url       (:url request)
                                             :params    (:query-params request)
                                             :timeout   request-timeout
                                             :response-time      (- (System/currentTimeMillis) time-before)})
          ; After Request hook
          _ (hook/execute-hook :after-request (get-after-ctx {:ctx before-hook-ctx
                                                              :status (:status result)
                                                              :response-time (- (System/currentTimeMillis) time-before)
                                                              :request request
                                                              :result result
                                                              :context context}))]
      (if (:debugging query-opts)
        (response-with-debug response request-map query-opts)
        response))))

(defn- build-error-response [error-data exception]
  (merge (select-keys error-data [:success :status :metadata :url :params :timeout :response-time])
         {:body {:message (get-error-message exception)}}))

(defn- request-error-callback [exception & {:keys [request
                                                   request-timeout
                                                   time-before
                                                   query-opts
                                                   request-map
                                                   before-hook-ctx
                                                   context]}]
  (let [error-status (get-error-status exception)
        log-data {:resource (:from request)
                  :timeout  request-timeout
                  :success  false}]
    (log/debug (assoc log-data :success false
                      :status error-status
                      :response-time (- (System/currentTimeMillis) time-before)))
    (let [error-data (assoc log-data :success false
                            :status error-status
                            :metadata (some-> request :metadata)
                            :method (:method request)
                            :url (some-> request :url)
                            :params    (:query-params request)
                            :response-time (- (System/currentTimeMillis) time-before)
                            :errordetail (pr-str exception))
          ; After Request hook
          _ (hook/execute-hook :after-request (get-after-ctx {:ctx before-hook-ctx
                                                              :status error-status
                                                              :response-time (- (System/currentTimeMillis) time-before)
                                                              :request request
                                                              :result error-data
                                                              :context context}))
          error-response (build-error-response error-data exception)]
      (log/warn error-data "Request failed")
      (if (:debugging query-opts)
        (response-with-debug error-response request-map query-opts)
        error-response))))

(defn- lower-case-keys [kv]
  (into {} (map (fn [[k v]]
                  {(clojure.string/lower-case k) v}) kv)))

(defn- append-request-headers-to-query-opts [request query-opts]
  (let [forward-headers (lower-case-keys (:forward-headers query-opts))
        with-headers   (lower-case-keys (:with-headers request))]

    (merge forward-headers with-headers)))

(defn- nil-header-to-empty-string [headers]
  (into {} (map (fn [[k v]]
                  [k (if (nil? v) "" v)]) headers)))

(defn- build-request-map [context request request-timeout valid-query-params headers time body-encoded poll-timeout]
  {:url                (:url request)
   :request-method     (:method request)
   :content-type       "application/json"
   :resource           (:from request)
   :connection-timeout (get-default :connection-timeout)
   :request-timeout    request-timeout
   :read-timeout       request-timeout
   :query-params       valid-query-params
   :headers            (nil-header-to-empty-string headers)
   :time               time
   :body               body-encoded
   :pool-timeout       poll-timeout
   :context            context})

; the total request timeout is enforced by request-with-timeout, the socket timeout
; only releases the connection of requests that were given up after a timeout
(def ^:private socket-timeout-margin 1000)

(defn- ->http-request [request-map]
  {:url                        (:url request-map)
   :method                     (:request-method request-map)
   :content-type               (:content-type request-map)
   :query-params               (:query-params request-map)
   :headers                    (:headers request-map)
   :body                       (:body request-map)
   :as                         :text
   :throw-exceptions           false
   :connection-manager         client-connection-pool
   :connection-timeout         (:connection-timeout request-map)
   :socket-timeout             (some-> (:read-timeout request-map) (+ socket-timeout-margin))
   :connection-request-timeout (:pool-timeout request-map)})

(defn- request-with-timeout
  "performs the http request, throwing a TimeoutException if the whole request
   takes longer than request-timeout"
  [http-request request-timeout]
  (if (nil? request-timeout)
    (http/request http-request)
    (let [response (future (http/request http-request))
          result   (try
                     (deref response request-timeout ::timeout)
                     (catch ExecutionException e
                       (throw (.getCause e))))]
      (when (= ::timeout result)
        (future-cancel response)
        (throw (TimeoutException. (str "Request timed out after " request-timeout "ms"))))
      result)))

(defn- lower-case-response-headers [response]
  (update response :headers lower-case-keys))

(defn- make-request [context request query-opts]
  (let [request         (parse-query-params request)
        time-before     (System/currentTimeMillis)
        request-timeout (if (nil? (:timeout request)) (:timeout query-opts) (:timeout request))
        request-map (build-request-map
                     context
                     request
                     request-timeout
                     (valid-query-params request query-opts)
                     (append-request-headers-to-query-opts request query-opts)
                     time-before
                     (some-> request :body json/encode)
                     (get-default :pool-timeout request-timeout))
         ; Before Request hook
        before-hook-ctx (hook/execute-hook :before-request request-map)
        request-map (assoc request-map :headers (-> (into {} (:outbound-headers-map before-hook-ctx))
                                                    (merge (:headers request-map))))]
    (log/debug request-map "Preparing request")
    (try
      (-> (->http-request request-map)
          (request-with-timeout request-timeout)
          (lower-case-response-headers)
          (request-respond-callback :request request
                                    :request-timeout request-timeout
                                    :query-opts query-opts
                                    :time-before time-before
                                    :request-map request-map
                                    :before-hook-ctx before-hook-ctx
                                    :context context))
      (catch Exception e
        (request-error-callback e
                                :request request
                                :request-timeout request-timeout
                                :query-opts query-opts
                                :time-before time-before
                                :request-map request-map
                                :before-hook-ctx before-hook-ctx
                                :context context)))))

(defn- create-skip-message [params]
  (str "The request was skipped due to missing {" (clojure.string/join ", " params) "} param value"))

(defn- get-empty-chained-params [request]
  (->> request
       (:query-params)
       (keep (fn [[k v]] (when (= :empty-chained v) k)))))

(defn verify-and-make-request
  "returns a future with the response of the request"
  [context request query-opts]
  (let [empty-chained-params (get-empty-chained-params request)]
    (if (empty? empty-chained-params)
      (future (make-request context request query-opts))
      (future {:status 400 :metadata (:metadata request) :body (create-skip-message empty-chained-params)}))))
