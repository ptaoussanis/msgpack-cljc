(ns taoensso.msgpack
  (:require [taoensso.msgpack.impl :as impl]))

(defn pack   [clj]   (impl/pack   clj))
(defn unpack [bytes] (impl/unpack bytes))

(defn pack-stream   [clj out-stream] (impl/pack-stream clj out-stream))
(defn unpack-stream [in-stream]      (impl/unpack-stream    in-stream))
