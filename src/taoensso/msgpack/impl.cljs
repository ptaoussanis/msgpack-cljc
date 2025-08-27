(ns taoensso.msgpack.impl
  (:require
   [goog.crypt]
   [goog.math.Long]
   [cljs.reader]
   [taoensso.msgpack.interfaces :as interfaces
    :refer [Packable CustomPackable pack-bytes]]))

;;;; Streams

(defn string->bytes [s] (js/Uint8Array. (.stringToUtf8ByteArray goog.crypt s)))
(defn bytes->string [bs]                (.utf8ByteArrayToString goog.crypt bs))

(defprotocol IStream
  (inc-offset!        [_ n])
  (resize-on-demand!  [_ n])
  (stream->uint8array [_]))

(defprotocol IInputStream
  (read-1     [_ n])
  (read-bytes [_ n])
  (read-u8    [_])
  (read-i8    [_])
  (read-u16   [_])
  (read-i16   [_])
  (read-u32   [_])
  (read-i32   [_])
  (read-i64   [_])
  (read-f32   [_])
  (read-f64   [_])
  (read-str   [_ n]))

(defprotocol IOutputStream
  (write-1   [_ buffer])
  (write-u8  [_ u8])
  (write-i8  [_ i8])
  (write-u16 [_ u16])
  (write-i16 [_ i16])
  (write-u32 [_ u32])
  (write-i32 [_ i32])
  (write-i64 [_ i64])
  (write-f64 [_ f64]))

(deftype MsgpackInputStream [bytes ^:unsynchronized-mutable offset]
  IStream
  (inc-offset!        [_ n] (set! offset (+ offset n)))
  (resize-on-demand!  [_ _] nil)
  (stream->uint8array [_  ] (js/Uint8Array. (.-buffer bytes)))

  IInputStream
  (read-1 [this n]
    (let [old-offset offset]
      (inc-offset! this n)
      (.slice (.-buffer bytes) old-offset offset)))

  (read-bytes [this n] (js/Uint8Array.    (read-1 this n)))
  (read-str   [this n] (bytes->string (read-bytes this n)))

  (read-u8    [this] (let [u8  (.getUint8   bytes offset)]       (inc-offset! this 1) u8))
  (read-i8    [this] (let [i8  (.getInt8    bytes offset)]       (inc-offset! this 1) i8))
  (read-u16   [this] (let [u16 (.getUint16  bytes offset)]       (inc-offset! this 2) u16))
  (read-i16   [this] (let [i16 (.getInt16   bytes offset false)] (inc-offset! this 2) i16))
  (read-u32   [this] (let [u32 (.getUint32  bytes offset false)] (inc-offset! this 4) u32))
  (read-i32   [this] (let [i32 (.getInt32   bytes offset false)] (inc-offset! this 4) i32))
  (read-f32   [this] (let [f32 (.getFloat32 bytes offset false)] (inc-offset! this 4) f32))
  (read-f64   [this] (let [f64 (.getFloat64 bytes offset false)] (inc-offset! this 8) f64))
  (read-i64   [this]
    (let [high-bits (.getInt32 bytes    offset    false)
          low-bits  (.getInt32 bytes (+ offset 4) false)]
      (inc-offset! this 8)
      (.toNumber (goog.math.Long. low-bits high-bits)))))

(deftype MsgpackOutputStream
  [^:unsynchronized-mutable bytes
   ^:unsynchronized-mutable offset]

  IStream
  (resize-on-demand! [_ n]
    (let [base (+ offset n)]
      (when (> base (.-byteLength bytes))
        (let [new-bytes (js/Uint8Array. (bit-or 0 (* 1.5 base)))
              old-bytes (js/Uint8Array. (.-buffer bytes))]
          (set! bytes (js/DataView. (.-buffer new-bytes)))
          (.set new-bytes old-bytes 0)))))

  (inc-offset!        [_ n] (set! offset (+ offset n)))
  (stream->uint8array [_  ] (js/Uint8Array. (.-buffer bytes) 0 offset))

  IOutputStream
  (write-1 [this buffer]
    (resize-on-demand! this (.-byteLength buffer))
    (if (instance? js/ArrayBuffer buffer)
      (.set (js/Uint8Array. (.-buffer bytes)) (js/Uint8Array. buffer) offset)
      (.set (js/Uint8Array. (.-buffer bytes)) buffer offset))
    (inc-offset! this (.-byteLength buffer)))

  (write-u8  [this u8]  (resize-on-demand! this 1) (.setUint8   bytes offset u8  false) (inc-offset! this 1))
  (write-i8  [this i8]  (resize-on-demand! this 1) (.setInt8    bytes offset i8  false) (inc-offset! this 1))
  (write-u16 [this u16] (resize-on-demand! this 2) (.setUint16  bytes offset u16 false) (inc-offset! this 2))
  (write-i16 [this i16] (resize-on-demand! this 2) (.setInt16   bytes offset i16 false) (inc-offset! this 2))
  (write-u32 [this u32] (resize-on-demand! this 4) (.setUint32  bytes offset u32 false) (inc-offset! this 4))
  (write-i32 [this i32] (resize-on-demand! this 4) (.setInt32   bytes offset i32 false) (inc-offset! this 4))
  (write-f64 [this f64] (resize-on-demand! this 8) (.setFloat64 bytes offset f64 false) (inc-offset! this 8))
  (write-i64 [this u64]
    ;; msgpack stores integers in big-endian
    (let [glong (.fromNumber goog.math.Long u64)]
      (write-i32 this ^js/Number (.getHighBits glong))
      (write-i32 this ^js/Number (.getLowBits  glong)))))

(defn  input-stream [input]  (MsgpackInputStream.  (js/DataView. input)  0))
(defn output-stream [output] (MsgpackOutputStream. (js/DataView. output) 0))

;;;;

(defn pack-byte-array
  [stream bytes]
  (let [n (.-byteLength bytes)]
    (cond
      (<= n 0xff)       (doto stream (write-u8 0xc4) (write-u8  n) (write-1 bytes))
      (<= n 0xffff)     (doto stream (write-u8 0xc5) (write-u16 n) (write-1 bytes))
      (<= n 0xffffffff) (doto stream (write-u8 0xc6) (write-u32 n) (write-1 bytes))
      :else (throw (js/Error. "bytes too large to pack")))))

;; Support only doubles
(defn pack-float [stream f] (doto stream (write-u8 0xcb) (write-f64 f)))
(defn pack-int   [stream i]
  (cond
    (<=   0 i  127)               (write-u8 stream i) ; +fixnum
    (<= -32 i   -1)               (write-i8 stream i) ; -fixnum
    (<=   0 i 0xff)               (doto stream (write-u8 0xcc) (write-u8  i)) ; uint 8
    (<=   0 i 0xffff)             (doto stream (write-u8 0xcd) (write-u16 i)) ; uint 16
    (<=   0 i 0xffffffff)         (doto stream (write-u8 0xce) (write-u32 i)) ; uint 32
    (<=   0 i 0xffffffffffffffff) (doto stream (write-u8 0xcf) (write-i64 i)) ; uint 64
    (<= -0x80               i -1) (doto stream (write-u8 0xd0) (write-i8  i)) ; int 8
    (<= -0x8000             i -1) (doto stream (write-u8 0xd1) (write-i16 i)) ; int 16
    (<= -0x80000000         i -1) (doto stream (write-u8 0xd2) (write-i32 i)) ; int 32
    (<= -0x8000000000000000 i -1) (doto stream (write-u8 0xd3) (write-i64 i)) ; int 64
    :else (throw (js/Error. (str "Integer value out of bounds: " i)))))

(defn pack-number [stream n]
  (if (integer? n)
    (pack-int   stream n)
    (pack-float stream n)))

(defn pack-string [stream s]
  (let [bytes (string->bytes s)
        len   (.-byteLength bytes)]
    (cond
      (<= len 0x1f)       (doto stream (write-u8 (bit-or 2r10100000 len)) (write-1 bytes))
      (<= len 0xff)       (doto stream (write-u8 0xd9) (write-u8    len)  (write-1 bytes))
      (<= len 0xffff)     (doto stream (write-u8 0xda) (write-u16   len)  (write-1 bytes))
      (<= len 0xffffffff) (doto stream (write-u8 0xdb) (write-u32   len)  (write-1 bytes))
      :else (throw (js/Error. "string too large to pack")))))

(declare pack)

(defn pack-coll [stream coll] (doseq [x coll] (pack-bytes x stream)))

(defprotocol     ICustomPackable (custom [_]))
(extend-protocol ICustomPackable
  PersistentHashSet (custom [this] (CustomPackable. 0x07 (pack (vec this))))
  cljs.core.Symbol  (custom [this] (CustomPackable. 0x04 (pack (str this))))
  Keyword           (custom [this] (CustomPackable. 0x03 (pack (.substring (str this) 1)))))

(defn pack-custom [o {:keys [byte-id ba-content]}]
  (let   [len (.-byteLength ba-content)]
    (case len
      1  (write-u8 o 0xd4)
      2  (write-u8 o 0xd5)
      4  (write-u8 o 0xd6)
      8  (write-u8 o 0xd7)
      16 (write-u8 o 0xd8)
      (cond
        (<= len 0xff)       (do (write-u8 o 0xc7) (write-u8  o len))
        (<= len 0xffff)     (do (write-u8 o 0xc8) (write-u16 o len))
        (<= len 0xffffffff) (do (write-u8 o 0xc9) (write-u32 o len))
        :else (throw (js/Error. "custom type too large to pack"))))
    (write-u8 o byte-id)
    (write-1  o ba-content)))

(defn pack-seq  [o seq]
  (let [len (count seq)]
    (cond
      (<= len 0xf)        (do (write-u8 o (bit-or 2r10010000 len))  (pack-coll o seq))
      (<= len 0xffff)     (do (write-u8 o 0xdc) (write-u16 o len)   (pack-coll o seq))
      (<= len 0xffffffff) (do (write-u8 o 0xdd) (write-u32 o len)   (pack-coll o seq))
      :else (throw (js/Error. "seq type too large to pack")))))

(defn pack-map    [o map]
  (let [len   (count map)
        pairs (interleave (keys map) (vals map))]
    (cond
      (<= len 0xf)        (do (write-u8 o (bit-or 2r10000000 len)) (pack-coll o pairs))
      (<= len 0xffff)     (do (write-u8 o 0xde) (write-u16 o len)  (pack-coll o pairs))
      (<= len 0xffffffff) (do (write-u8 o 0xdf) (write-u32 o len)  (pack-coll o pairs))
      :else (throw (js/Error. "map type too large to pack")))))

(extend-protocol Packable
  nil                (pack-bytes [_    o] (write-u8    o       0xc0))
  boolean            (pack-bytes [b    o] (write-u8    o (if b 0xc3 0xc2)))
  number             (pack-bytes [n    o] (pack-number o n))
  string             (pack-bytes [s    o] (pack-string o s))
  CustomPackable     (pack-bytes [cp   o] (pack-custom o cp))

  Keyword            (pack-bytes [kw   o] (pack-bytes (custom kw)   o))
  Symbol             (pack-bytes [sym  o] (pack-bytes (custom sym)  o))
  PersistentHashSet  (pack-bytes [hset o] (pack-bytes (custom hset) o))
  PersistentVector   (pack-bytes [seq  o] (pack-seq o seq))
  PersistentArrayMap (pack-bytes [amap o] (pack-map o amap))
  PersistentHashMap  (pack-bytes [hmap o] (pack-map o hmap))
  List               (pack-bytes [seq  o] (pack-seq o seq))
  EmptyList          (pack-bytes [seq  o] (pack-seq o seq))
  LazySeq            (pack-bytes [seq  o] (pack-seq o (vec seq)))

  js/Uint8Array      (pack-bytes [u8   o] (pack-byte-array o u8))
  js/Int32Array      (pack-bytes [ar   o] (pack-custom     o {:byte-id 101 :ba-content (.-buffer ar)}))
  js/Float32Array    (pack-bytes [ar   o] (pack-custom     o {:byte-id 102 :ba-content (.-buffer ar)}))
  js/Float64Array    (pack-bytes [ar   o] (pack-custom     o {:byte-id 103 :ba-content (.-buffer ar)}))
  js/Date            (pack-bytes [d    o] (pack-string     o (.toISOString d))))

(declare unpack-stream)

(defn unpack-n [stream n]
  (let [v (transient [])]
    (dotimes [_ n] (conj! v (unpack-stream stream)))
    (persistent! v)))

(defn unpack-map [stream n] (apply hash-map (unpack-n stream (* 2 n))))

(declare unpack-ext)

(defn unpack-stream [stream]
  (let [byte (read-u8 stream)]
    (case byte
      0xc0 nil
      0xc2 false
      0xc3 true
      0xc4 (read-bytes stream (read-u8  stream))
      0xc5 (read-bytes stream (read-u16 stream))
      0xc6 (read-bytes stream (read-u32 stream))
      0xc7 (unpack-ext stream (read-u8  stream))
      0xc8 (unpack-ext stream (read-u16 stream))
      0xc9 (unpack-ext stream (read-u32 stream))
      0xca (read-f32 stream)
      0xcb (read-f64 stream)
      0xcc (read-u8  stream)
      0xcd (read-u16 stream)
      0xce (read-u32 stream)
      0xcf (read-i64 stream)
      0xd0 (read-i8  stream)
      0xd1 (read-i16 stream)
      0xd2 (read-i32 stream)
      0xd3 (read-i64 stream)
      0xd4 (unpack-ext stream 1)
      0xd5 (unpack-ext stream 2)
      0xd6 (unpack-ext stream 4)
      0xd7 (unpack-ext stream 8)
      0xd8 (unpack-ext stream 16)
      0xd9 (read-str stream (read-u8  stream))
      0xda (read-str stream (read-u16 stream))
      0xdb (read-str stream (read-u32 stream))
      0xdc (unpack-n        stream (read-u16 stream))
      0xdd (unpack-n        stream (read-u32 stream))
      0xde (unpack-map      stream (read-u16 stream))
      0xdf (unpack-map      stream (read-u32 stream))
      (cond
        (= (bit-and 2r11100000 byte) 2r11100000) byte
        (= (bit-and 2r10000000 byte) 0)          byte
        (= (bit-and 2r11100000 byte) 2r10100000) (read-str stream (bit-and 2r11111 byte))
        (= (bit-and 2r11110000 byte) 2r10010000) (unpack-n        stream (bit-and 2r1111  byte))
        (= (bit-and 2r11110000 byte) 2r10000000) (unpack-map      stream (bit-and 2r1111  byte))
        :else (throw (js/Error. "invalid msgpack stream"))))))

(defn keyword-deserializer [bytes] (keyword       (unpack-stream (input-stream bytes))))
(defn  symbol-deserializer [bytes] (symbol        (unpack-stream (input-stream bytes))))
(defn    char-deserializer [bytes]                (unpack-stream (input-stream bytes)))
(defn   ratio-deserializer [bytes] (let [[n d]    (unpack-stream (input-stream bytes))] (/ n d)))
(defn     set-deserializer [bytes] (set           (unpack-stream (input-stream bytes))))
(defn    date-deserializer [bytes] (let [date-str (unpack-stream (input-stream bytes))] (cljs.reader/parse-timestamp date-str)))

(defn    int-array-deserializer [buffer] (js/Int32Array.   buffer))
(defn  float-array-deserializer [buffer] (js/Float32Array. buffer))
(defn double-array-deserializer [buffer] (js/Float64Array. buffer))
(defn   byte-array-deserializer [buffer] (js/Uint8Array.   buffer))

(defn unpack-ext [stream n]
  (let   [byte-id (read-u8 stream)]
    (case byte-id
      3   (keyword-deserializer      (read-1 stream n))
      4   (symbol-deserializer       (read-1 stream n))
      5   (char-deserializer         (read-1 stream n))
      6   (ratio-deserializer        (read-1 stream n))
      7   (set-deserializer          (read-1 stream n))
      100 (date-deserializer         (read-1 stream n))
      101 (int-array-deserializer    (read-1 stream n))
      102 (float-array-deserializer  (read-1 stream n))
      103 (double-array-deserializer (read-1 stream n))
      104 (byte-array-deserializer   (read-1 stream n))
      (interfaces/unpack-custom
        (interfaces/CustomPackable. byte-id (read-1 stream n))))))

(defn pack
  ([       clj] (let [os (output-stream (js/ArrayBuffer. 2047))] (pack-bytes clj os) (stream->uint8array os)))
  ([output clj] (let [os (output-stream output)]                 (pack-bytes clj os))))

(defn unpack [packed]
  (cond
    (instance? js/Uint8Array packed) (unpack-stream (input-stream (.-buffer packed)))
    :else                            (unpack-stream (input-stream           packed))))
