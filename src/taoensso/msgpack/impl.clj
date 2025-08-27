(ns taoensso.msgpack.impl
  (:require [taoensso.msgpack.interfaces :as i :refer [Packable pack-bytes]])
  (:import
   [taoensso.msgpack.interfaces CustomPackable]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.charset StandardCharsets]
   [java.io
    ByteArrayInputStream ByteArrayOutputStream DataInput DataOutput
    DataInputStream DataOutputStream InputStream OutputStream]))

(defmacro cond-let
  {:clj-kondo/lint-as 'clojure.core/let}
  [bindings & clauses]
  `(let ~bindings (cond ~@clauses)))

(defn- pack-raw [^bytes bytes ^DataOutput s]
  (cond-let [len (count bytes)]
    (<= len 0x1f)       (do (.writeByte s (bit-or 2r10100000 len))  (.write s bytes))
    (<= len 0xffff)     (do (.writeByte s 0xda) (.writeShort s len) (.write s bytes))
    (<= len 0xffffffff) (do (.writeByte s 0xdb) (.writeInt   s len) (.write s bytes))))

(defn- pack-str [^bytes bytes ^DataOutput s]
  (cond-let [len (count bytes)]
    (<= len 0x1f)       (do (.writeByte s (bit-or 2r10100000 len))  (.write s bytes))
    (<= len 0xff)       (do (.writeByte s 0xd9) (.writeByte  s len) (.write s bytes))
    (<= len 0xffff)     (do (.writeByte s 0xda) (.writeShort s len) (.write s bytes))
    (<= len 0xffffffff) (do (.writeByte s 0xdb) (.writeInt   s len) (.write s bytes))))

(defn- pack-byte-array [^bytes bytes ^DataOutput s]
  (cond-let [len (count bytes)]
    (<= len 0xff)       (do (.writeByte s 0xc4) (.writeByte  s len) (.write s bytes))
    (<= len 0xffff)     (do (.writeByte s 0xc5) (.writeShort s len) (.write s bytes))
    (<= len 0xffffffff) (do (.writeByte s 0xc6) (.writeInt   s len) (.write s bytes))))

(defn- pack-int
  "Pack integer using the most compact representation"
  [n ^DataOutput s]
  (cond
    (<=   0 n 127)                    (.writeByte s n) ; +fixnum
    (<= -32 n -1)                     (.writeByte s n) ; -fixnum
    (<=   0 n 0xff)               (do (.writeByte s 0xcc) (.writeByte  s n)) ; uint 8
    (<=   0 n 0xffff)             (do (.writeByte s 0xcd) (.writeShort s n)) ; uint 16
    (<=   0 n 0xffffffff)         (do (.writeByte s 0xce) (.writeInt   s (unchecked-int  n))) ; uint 32
    (<=   0 n 0xffffffffffffffff) (do (.writeByte s 0xcf) (.writeLong  s (unchecked-long n))) ; uint 64
    (<= -0x80               n -1) (do (.writeByte s 0xd0) (.writeByte  s n)) ; int 8
    (<= -0x8000             n -1) (do (.writeByte s 0xd1) (.writeShort s n)) ; int 16
    (<= -0x80000000         n -1) (do (.writeByte s 0xd2) (.writeInt   s n)) ; int 32
    (<= -0x8000000000000000 n -1) (do (.writeByte s 0xd3) (.writeLong  s n)) ; int 64
    :else (throw (IllegalArgumentException. (str "Integer value out of bounds: " n)))))

(defn- pack-coll [coll ^DataOutput s] (doseq [item coll] (pack-bytes item s)))

(def ^:private CLASS-OF-BYTE-ARRAY           (class (java.lang.reflect.Array/newInstance Byte 0)))
(def ^:private CLASS-OF-PRIMITIVE-BYTE-ARRAY (Class/forName "[B"))

;; Array of java.lang.Byte (boxed)
(extend CLASS-OF-BYTE-ARRAY           Packable {:pack-bytes (fn [a     ^DataOutput s] (pack-bytes (byte-array a) s))})
(extend CLASS-OF-PRIMITIVE-BYTE-ARRAY Packable {:pack-bytes (fn [bytes ^DataOutput s] (pack-byte-array bytes     s))})

(extend-protocol Packable
  nil (pack-bytes [_ ^DataOutput s] (.writeByte s 0xc0))
  java.lang.Boolean
  (pack-bytes
    [bool ^DataOutput s]
    (if bool
      (.writeByte s 0xc3)
      (.writeByte s 0xc2)))

  java.lang.Byte (pack-bytes [n ^DataOutput s] (pack-int n s))
  java.lang.Short (pack-bytes [n ^DataOutput s] (pack-int n s))
  java.lang.Integer (pack-bytes [n ^DataOutput s] (pack-int n s))
  java.lang.Long (pack-bytes [n ^DataOutput s] (pack-int n s))
  java.math.BigInteger (pack-bytes [n ^DataOutput s] (pack-int n s))
  clojure.lang.BigInt (pack-bytes [n ^DataOutput s] (pack-int n s))
  java.lang.Float (pack-bytes [f ^DataOutput s] (do (.writeByte s 0xca) (.writeFloat s f)))
  java.lang.Double (pack-bytes [d ^DataOutput s] (do (.writeByte s 0xcb) (.writeDouble s d)))
  java.math.BigDecimal (pack-bytes [d ^DataOutput s] (pack-bytes (.doubleValue d) s))

  java.lang.String
  (pack-bytes
    [str ^DataOutput s]
    (let [bytes (.getBytes ^String str StandardCharsets/UTF_8)]
      (pack-str bytes s)))

  CustomPackable
  (pack-bytes [cp ^DataOutput s]
    (let [bid       (.-byte-id    cp)
          ^bytes ba (.-ba-content cp)
          len       (alength ba)]

      (case len
        1  (.writeByte s 0xd4)
        2  (.writeByte s 0xd5)
        4  (.writeByte s 0xd6)
        8  (.writeByte s 0xd7)
        16 (.writeByte s 0xd8)
        (cond
          (<= len 0xff)       (do (.writeByte s 0xc7) (.writeByte  s len))
          (<= len 0xffff)     (do (.writeByte s 0xc8) (.writeShort s len))
          (<= len 0xffffffff) (do (.writeByte s 0xc9) (.writeInt   s len))))

      (.writeByte s bid)
      (.write     s ba)))

  clojure.lang.Sequential
  (pack-bytes [seq ^DataOutput s]
    (cond-let [len (count seq)]
      (<= len 0xf)        (do (.writeByte s (bit-or 2r10010000 len))  (pack-coll seq s))
      (<= len 0xffff)     (do (.writeByte s 0xdc) (.writeShort s len) (pack-coll seq s))
      (<= len 0xffffffff) (do (.writeByte s 0xdd) (.writeInt   s len) (pack-coll seq s))))

  clojure.lang.IPersistentMap
  (pack-bytes [map ^DataOutput s]
    (cond-let [len   (count map)
               pairs (interleave (keys map) (vals map))]

      (<= len 0xf)        (do (.writeByte s (bit-or 2r10000000 len))  (pack-coll pairs s))
      (<= len 0xffff)     (do (.writeByte s 0xde) (.writeShort s len) (pack-coll pairs s))
      (<= len 0xffffffff) (do (.writeByte s 0xdf) (.writeInt   s len) (pack-coll pairs s)))))

;; Note: the extensions below are not in extend-protocol above because of
;; a Clojure bug. See http://dev.clojure.org/jira/browse/CLJ-1381

(defn- read-uint8  [^DataInput data-input] (.readUnsignedByte data-input))
(defn- read-uint16 [^DataInput data-input] (.readUnsignedShort data-input))
(defn- read-uint32 [^DataInput data-input] (bit-and 0xffffffff (.readInt data-input)))
(defn- read-uint64 [^DataInput data-input]
  (let [n (.readLong data-input)]
    (if (<= 0 n Long/MAX_VALUE)
      n
      (.and (biginteger n) (biginteger 0xffffffffffffffff)))))

(defn- read-bytes [n ^DataInput data-input]
  (let [bytes (byte-array n)]
    (.readFully data-input bytes)
    bytes))

(defn- read-str [n ^DataInput data-input]
  (let [bytes (read-bytes n data-input)]
    (String. ^bytes bytes StandardCharsets/UTF_8)))

(declare unpack-stream)

(defn- unpack-custom [n ^DataInput data-input] (i/unpack-custom (i/->CustomPackable (.readByte data-input) (read-bytes n data-input))))
(defn- unpack-n      [n ^DataInput data-input]
  (loop [i 0
         v (transient [])]
    (if (< i n)
      (recur
        (unchecked-inc i)
        (conj! v (unpack-stream data-input)))
      (persistent! v))))

(defn- unpack-map [n ^DataInput data-input]
  (loop [i 0
         m (transient {})]
    (if (< i n)
      (recur
        (unchecked-inc i)
        (assoc! m
          (unpack-stream data-input)
          (unpack-stream data-input)))
      (persistent! m))))

(defn unpack-stream
  [^DataInput data-input]
  (cond-let [byte (.readUnsignedByte data-input)]
    (= byte 0xc0) nil   ; nil  format family
    (= byte 0xc2) false ; bool format family
    (= byte 0xc3) true

    ;; int format family
    (= (bit-and 2r11100000 byte) 2r11100000) (unchecked-byte byte)
    (= (bit-and 2r10000000 byte) 0)          (unchecked-byte byte)
    (= byte 0xcc) (read-uint8  data-input)
    (= byte 0xcd) (read-uint16 data-input)
    (= byte 0xce) (read-uint32 data-input)
    (= byte 0xcf) (read-uint64 data-input)
    (= byte 0xd0) (.readByte   data-input)
    (= byte 0xd1) (.readShort  data-input)
    (= byte 0xd2) (.readInt    data-input)
    (= byte 0xd3) (.readLong   data-input)

    ;; float format family
    (= byte 0xca) (.readFloat  data-input)
    (= byte 0xcb) (.readDouble data-input)

    ;; str format family
    (= (bit-and 2r11100000 byte) 2r10100000) (let [n (bit-and 2r11111 byte)] (read-str n data-input))
    (= byte 0xd9) (read-str (read-uint8  data-input) data-input)
    (= byte 0xda) (read-str (read-uint16 data-input) data-input)
    (= byte 0xdb) (read-str (read-uint32 data-input) data-input)

    ;; bin format family
    (= byte 0xc4) (read-bytes (read-uint8  data-input) data-input)
    (= byte 0xc5) (read-bytes (read-uint16 data-input) data-input)
    (= byte 0xc6) (read-bytes (read-uint32 data-input) data-input)

    ;; custom format family
    (= byte 0xd4) (unpack-custom 1            data-input)
    (= byte 0xd5) (unpack-custom 2            data-input)
    (= byte 0xd6) (unpack-custom 4            data-input)
    (= byte 0xd7) (unpack-custom 8            data-input)
    (= byte 0xd8) (unpack-custom 16           data-input)
    (= byte 0xc7) (unpack-custom (read-uint8  data-input) data-input)
    (= byte 0xc8) (unpack-custom (read-uint16 data-input) data-input)
    (= byte 0xc9) (unpack-custom (read-uint32 data-input) data-input)

    ;; array format family
    (= (bit-and 2r11110000 byte) 2r10010000) (unpack-n (bit-and 2r1111 byte) data-input)
    (= byte 0xdc) (unpack-n (read-uint16 data-input) data-input)
    (= byte 0xdd) (unpack-n (read-uint32 data-input) data-input)

    ;; map format family
    (= (bit-and 2r11110000 byte) 2r10000000) (unpack-map (bit-and 2r1111 byte)    data-input)
    (= byte 0xde)                            (unpack-map (read-uint16 data-input) data-input)
    (= byte 0xdf)                            (unpack-map (read-uint32 data-input) data-input)))

;; Array of java.lang.Byte (boxed)
(extend CLASS-OF-BYTE-ARRAY           Packable {:pack-bytes (fn [a     ^DataOutput s] (pack-bytes (byte-array a) s))})
(extend CLASS-OF-PRIMITIVE-BYTE-ARRAY Packable {:pack-bytes (fn [bytes ^DataOutput s] (pack-byte-array bytes     s))})

(defn pack
  (^bytes [clj] (let [baos (ByteArrayOutputStream.)] (pack baos clj) (.toByteArray baos)))
  ([output clj]
   (cond
     (instance? DataOutput   output) (pack-bytes clj                                  output)
     (instance? OutputStream output) (pack-bytes clj (DataOutputStream. ^OutputStream output))
     :else
     (throw
       (ex-info "Pack failed: unexpected `output` type"
         {:given {:value output, :type (type output)}
          :expected '#{DataOutput OutputStream}})))))

(defn unpack [packed]
  (cond
    (bytes?                packed) (unpack-stream (DataInputStream. (ByteArrayInputStream.             packed)))
    (instance? DataInput   packed) (unpack-stream                                                      packed)
    (instance? InputStream packed) (unpack-stream (DataInputStream.                                    packed))
    (seq?                  packed) (unpack-stream (DataInputStream. (ByteArrayInputStream. (byte-array packed))))
    :else
    (throw
      (ex-info "Unpack failed: unexpected `packed` type"
        {:given {:value packed, :type (type packed)}
         :expected '#{bytes DataInput InputStream }}))))

;;;;

(defmacro extend-custom
  [byte-id class
   [_   pack-args   pack-form]
   [_ unpack-args unpack-form]]

  `(let [byte-id# ~byte-id]
     (assert (<= 0 byte-id# 127) "[-1, -128]: reserved for future pre-defined extensions.")
     (extend-protocol Packable ~class (pack-bytes [~@pack-args o#] (pack-bytes (CustomPackable. byte-id# ~pack-form) o#)))
     (defmethod i/unpack-custom byte-id# [cp#]
       (let [~@unpack-args (get cp# :ba-content)]
         ~unpack-form))))

(defn- keyword->str
  "Convert keyword to string with namespace preserved.
  Example: :A/A => \"A/A\""
  [k]
  (subs (str k) 1))

(extend-custom 3 clojure.lang.Keyword
  (pack   [k]  (pack (keyword->str k)))
  (unpack [ba] (keyword (unpack ba))))

(extend-custom 4 clojure.lang.Symbol
  (pack   [s]  (pack (str s)))
  (unpack [ba] (symbol (unpack ba))))

(extend-custom 5 java.lang.Character
  (pack   [c]  (pack (str c)))
  (unpack [ba] (aget (char-array (unpack ba)) 0)))

(extend-custom 6 clojure.lang.Ratio
  (pack   [r]  (pack [(numerator r) (denominator r)]))
  (unpack [ba] (let [[n d] (unpack ba)] (/ n d))))

(extend-custom 7 clojure.lang.IPersistentSet
  (pack   [s]  (pack (seq s)))
  (unpack [ba] (set (unpack ba))))

(extend-custom 101 (class (int-array 0))
  (pack [ar]
    (let     [bb (ByteBuffer/allocate (* 4 (count ar)))]
      (.order bb (ByteOrder/nativeOrder))
      (doseq [v ar] (.putInt bb v))
      (do           (.array  bb))))

  (unpack [ba]
    (let [bb     (ByteBuffer/wrap ba)
          _      (.order      bb (ByteOrder/nativeOrder))
          int-bb (.asIntBbfer bb)
          int-ar (int-array (.limit int-bb))]
      (.get int-bb int-ar)
      (do          int-ar))))

(extend-custom 102 (class (float-array 0))
  (pack [ar]
    (let     [bb (ByteBuffer/allocate (* 4 (count ar)))]
      (.order bb (ByteOrder/nativeOrder))
      (doseq [f ar] (.putFloat bb f))
      (do           (.array    bb))))

  (unpack [ba]
    (let [bb       (ByteBuffer/wrap ba)
          _        (.order        bb (ByteOrder/nativeOrder))
          float-bb (.asFloatBbfer bb)
          float-ar (float-array (.limit float-bb))]
      (.get float-bb float-ar)
      (do            float-ar))))

(extend-custom 103 (class (double-array 0))
  (pack [ar]
    (let     [bb (ByteBuffer/allocate (* 8 (count ar)))]
      (.order bb (ByteOrder/nativeOrder))
      (doseq [v ar] (.putDouble bb v))
      (do           (.array     bb))))

  (unpack [ba]
    (let [bb        (ByteBuffer/wrap ba)
          _         (.order         bb (ByteOrder/nativeOrder))
          double-bb (.asDoubleBbfer bb)
          double-ar (double-array (.limit double-bb))]
      (.get double-bb double-ar)
      (do             double-ar))))
