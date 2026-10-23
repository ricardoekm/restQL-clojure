(ns restql.core.api.restql
  (:require [restql.core.runner.core :as runner]
            [restql.core.validator.core :as validator]
            [restql.core.transformations.select :as select]
            [restql.core.transformations.aggregation :as aggregation]
            [restql.hooks.core :as hook]
            [restql.core.api.response-builder :as response-builder]
            [restql.core.encoders.core :as encoders]
            [restql.parser.core :as parser]
            [clojure.walk :refer [stringify-keys]]
            [environ.core :refer [env]]))

(def default-values {:query-resource-timeout 5000
                     :query-global-timeout 30000})

(defn get-default [key]
  (if (contains? env key) (read-string (env key)) (default-values key)))

(defn- validate-query [context query-parsed]
  (->> query-parsed
       (validator/validate context)
       (partition 2)))

(defn- extract-result
  "builds the query response from the runner result. Timeout and exception results are returned as is"
  [parsed-query result]
  (if (map? result)
    result
    (->> (response-builder/build (reduce (fn [res [key value]] (assoc res key value))
                                         {}
                                         result))
         (select/from-result (flatten parsed-query))
         (aggregation/aggregate parsed-query))))

(defn get-default-encoders []
  (encoders/get-default-encoders))

(defn- set-default-query-options [query-options]
  (into {:timeout        (get-default :query-resource-timeout)
         :global-timeout (get-default :query-global-timeout)} query-options))

(defn- query-timeout [parsed-query query-opts]
  (if-let [timeout (-> parsed-query meta :timeout)]
    timeout
    (:global-timeout query-opts)))

(defn execute-query-future
  "returns a future with the query result, {:error :timeout} if the query timeout
   is reached or an exception map ({:type \"exception\" ...}) if an exception occurs"
  [& {:keys [context mappings encoders query query-opts]}]
  (let [; Before query hook
        _ (hook/execute-hook :before-query {:query query
                                            :query-options query-opts})

        time-before (System/currentTimeMillis)

        ; Validating query
        query-opts (set-default-query-options query-opts)
        validated-query (validate-query {:mappings mappings :encoders encoders} query)

        context (assoc context
                       :encoders encoders
                       :mappings mappings)

        run-result (runner/run context validated-query query-opts (query-timeout query query-opts))]
    (future
      (let [query-result (extract-result validated-query @run-result)
            ; After query hook
            _ (hook/execute-hook :after-query {:query-options query-opts
                                               :query         query
                                               :result        query-result
                                               :response-time (- (System/currentTimeMillis) time-before)})]
        query-result))))

(defn execute-parsed-query [& {:keys [context mappings encoders query query-opts]}]
  @(execute-query-future :context context
                         :mappings mappings
                         :encoders encoders
                         :query query
                         :query-opts query-opts))

(defn execute-query [& {:keys [context mappings encoders query params options]}]
  (let [parsed-query (parser/parse-query query :context (stringify-keys params))]
    (execute-parsed-query :context context
                          :mappings mappings
                          :encoders encoders
                          :query parsed-query
                          :query-opts options)))

(defn execute-parsed-query-async [& {:keys [context mappings encoders query query-opts callback]}]
  (future
    (callback @(execute-query-future :context context
                                     :mappings mappings
                                     :encoders encoders
                                     :query query
                                     :query-opts query-opts))))

(defn execute-query-async [& {:keys [context mappings encoders query params options callback]}]
  (let [parsed-query (parser/parse-query query :context (stringify-keys params))]
    (execute-parsed-query-async :context context
                                :mappings mappings
                                :encoders encoders
                                :query parsed-query
                                :query-opts options
                                :callback callback)))
