(ns restql.core.runner.executor
  (:require [restql.core.runner.request :refer [verify-and-make-request]]))

(defn- extract-result-keeping-futures-order [futures]
  (future (mapv deref futures)))

(defn- query-and-join [context requests query-opts]
  (let [operation (if (sequential? (first requests)) query-and-join verify-and-make-request)]
    (->> requests
         (mapv #(operation context % query-opts))
         (extract-result-keeping-futures-order))))

(defn- single-request-not-multiplexed? [requests]
  (and
   (= 1 (count requests))
   (not (sequential? (first requests)))
   (not (:multiplexed (first requests)))))

(defn do-request
  "returns a future with the result of the requests. Exceptions thrown while
   preparing the requests are propagated to the caller"
  [context requests {:keys [_debugging] :as query-opts}]
  (if (single-request-not-multiplexed? requests)
    (verify-and-make-request context (first requests) query-opts)
    (query-and-join context requests query-opts)))
