(defproject com.taoensso.forks/msgpack-cljc "2.1.0-SNAPSHOT"
  :description "Clj/s MessagePack implementation"
  :url "https://github.com/taoensso/msgpack-cljc"

  :license
  {:name "The MIT License (MIT)"
   :url  "http://opensource.org/licenses/MIT"}

  :dependencies []
  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojurescript "1.12.42"]
                             [org.clojure/clojure       "1.12.2"]]}
   :c1.12    {:dependencies [[org.clojure/clojure       "1.12.2"]]}
   :c1.11    {:dependencies [[org.clojure/clojure       "1.11.4"]]}
   :c1.10    {:dependencies [[org.clojure/clojure       "1.10.3"]]}

   :non-utf8-encoding {:jvm-opts ["-Dfile.encoding=ISO-8859-1"]}
   :dev
   {:global-vars
    {*warn-on-reflection* true
     *unchecked-math*     :warn-on-boxed}

    :dependencies
    [[org.clojure/test.check "1.1.1"]]

    :plugins
    [[lein-pprint    "1.3.2"]
     [lein-ancient   "0.7.0"]
     [lein-cljsbuild "1.1.8"]]}}

  :cljsbuild
  {:test-commands {"node" ["node" "target/test.js"]}
   :builds
   [{:id :main
     :source-paths ["src"]
     :compiler
     {:output-to "target/main.js"
      :optimizations :advanced}}

    {:id :test
     :source-paths ["src" "test"]
     :compiler
     {:output-to "target/test.js"
      :target :nodejs
      :optimizations :simple}}]}

  :aliases
  {"start-dev"  ["with-profile" "+dev" "repl" ":headless"]
   "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib" ["do" ["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"   ["with-profile" "+c1.12:+c1.11:+c1.10" "test"]
   "test-cljs"  ["with-profile" "+c1.12" "cljsbuild"   "test"]
   "test-all"   ["do" ["clean"] ["test-clj"] ["test-cljs"]]})
