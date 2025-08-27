(ns msgpack.core
  (:require [msgpack.pack :as pack]))

(defn pack   [obj]   (pack/pack   obj))
(defn unpack [bytes] (pack/unpack bytes))

(defn pack-stream   [obj stream] (pack/pack-stream obj stream))
(defn unpack-stream [in-stream]  (pack/unpack-stream in-stream))
