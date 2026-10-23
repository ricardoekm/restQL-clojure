(ns restql.core.runner.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [restql.core.runner.core :as core])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(def is-done? #'core/is-done?)
(def can-request? #'core/can-request?)
(def all-that-can-request #'core/all-that-can-request)

(deftest is-done?-test
  (is (=
       true
       (is-done? [:cart {:with {:id "123"}}]
                 {:done [[:cart {:with {:id "123"}}]]
                  :requested []
                  :to-do []})))

  (is (=
       false
       (is-done? [:cart {:with {:id "123"}}]
                 {:done [[:customer {:with {:id "123"}}]]
                  :requested []
                  :to-do []}))))

(deftest can-request?-test
  (is (=
       true
       (can-request? [:cart {:with {:id "123"}}]
                     {:done []})))

  (is (=
       true
       (can-request? [:cart {:with {:id [:checkout :cartId]}}]
                     {:done [[:checkout {:body {:id "321"}}]]})))

  (is (=
       false
       (can-request? [:cart {:with {:id [:checkout :cartId]}}]
                     {:done []}))))

(deftest all-that-can-request-test
  (is (=
       (seq [[:customer {:with {:id [:cart :id]}}]
             [:article  {:with {:id "123"}}]])

       (all-that-can-request {:done [[:cart {:body []}]]
                              :requested []
                              :to-do [[:customer {:with {:id [:cart :id]}}]
                                      [:address  {:with {:customer [:customer :id]}}]
                                      [:article  {:with {:id "123"}}]]}))))

(def do-run #'core/do-run)

(defn- far-deadline [] (+ (System/currentTimeMillis) 10000))

(deftest do-run-test
  (testing "Dispatch requests and return done results when all queries are done"
    (let [results    (LinkedBlockingQueue.)
          dispatched (atom [])
          query      [[:hero {:from :hero :method :get}]
                      [:sidekick {:from :sidekick :method :get :with {:id [:hero :id]}}]]
          dispatch!  (fn [{:keys [to-do]}]
                       (swap! dispatched conj (first to-do))
                       (.put results [(first to-do) {:status 200}]))]
      (is (= [[:hero {:status 200}] [:sidekick {:status 200}]]
             (do-run query results (far-deadline) dispatch!)))
      (is (= [:hero :sidekick] @dispatched))))

  (testing "Return done results when there is nothing to request"
    (is (= [] (do-run [] (LinkedBlockingQueue.) (far-deadline) (fn [_])))))

  (testing "If an exception occurs stop and return it"
    (let [results    (LinkedBlockingQueue.)
          dispatched (atom 0)
          query      [[:hero {:from :hero :method :get}]
                      [:sidekick {:from :sidekick :method :get :with {:id [:hero :id]}}]]
          dispatch!  (fn [_]
                       (swap! dispatched inc)
                       (.put results [::core/exception (Exception. "exceptional")]))
          result     (do-run query results (far-deadline) dispatch!)]
      (is (= "exception" (:type result)))
      (is (= "exceptional" (:message result)))
      (is (= 1 @dispatched))))

  (testing "If the deadline is reached stop and return timeout"
    (let [dispatched (atom 0)]
      (is (= {:error :timeout}
             (do-run [[:hero {:from :hero :method :get}]]
                     (LinkedBlockingQueue.)
                     (+ (System/currentTimeMillis) 10)
                     (fn [_] (swap! dispatched inc)))))
      (is (= 1 @dispatched)))))
