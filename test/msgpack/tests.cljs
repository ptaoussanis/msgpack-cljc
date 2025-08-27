(ns msgpack.tests
  (:require
   [clojure.test :as test :refer [deftest testing is]]
   [msgpack.core :as msg]
   [msgpack.interface :refer [->Extended]]))

(defmethod test/report [:cljs.test/default :end-run-tests] [m]
  (when-not (test/successful? m)
    ;; Trigger non-zero `lein test-cljs` exit code for CI
    (throw (ex-info "ClojureScript tests failed" {}))))

(deftest nil-test
  (testing "nil"
    (is (= nil (msg/unpack (msg/pack nil))))))

(deftest boolean-test
  (testing "booleans"
    (is (= true  (msg/unpack (msg/pack true))))
    (is (= false (msg/unpack (msg/pack false))))))

(deftest number-test
  (testing "numbers"
    (is (=  123 (msg/unpack (msg/pack  123))))
    (is (= -123 (msg/unpack (msg/pack -123))))
    (is (= 1.23 (msg/unpack (msg/pack 1.23))))))

(deftest string-test
  (testing "strings"
    (is (= "hello" (msg/unpack (msg/pack "hello"))))
    (is (= ""      (msg/unpack (msg/pack ""))))))

(deftest collection-test
  (testing "collections"
    (let [v [1 2 3]]     (is (= v (msg/unpack (msg/pack v)))))
    (let [m {:a 1 :b 2}] (is (= m (msg/unpack (msg/pack m)))))))

(test/run-tests)
