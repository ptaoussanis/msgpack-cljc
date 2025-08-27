(ns taoensso.msgpack.interfaces)

(defprotocol     Packable (pack-bytes [clj output]))
(defrecord CustomPackable [byte-id ba-content])

(defmulti  unpack-custom      (fn [custom-packable] (get custom-packable :byte-id)))
(defmethod unpack-custom :default [custom-packable]      custom-packable)
