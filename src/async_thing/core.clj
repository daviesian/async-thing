(ns async-thing.core
  (:use [clojure.core.async]))


(let [reply-channels (atom {})]
  (defn get-reply-channel [request-id]

    (get (swap! reply-channels (fn [cs]
                                 (if (get cs request-id)
                                   cs
                                   (assoc cs request-id (chan))))) request-id))

  (defn clear-reply-channel [request-id]
    (let [c (get @reply-channels request-id)]
      (loop [x (poll! c)]
        (when x (recur (poll! c))))
      (close! c))))


(defn open-socket [location]
  (println "Opening socket to" location "...")
  (Thread/sleep 1000)
  (println "Done")
  (let [recv (chan)]
    {:send (fn [msg]
             (println "SEND" (:request-id msg) ":" (:request msg))
             (thread
               (Thread/sleep (rand-int 1000))
               (>!! recv {:request-id (:request-id msg)
                          :result     (str "REPLY TO " (:request msg))})))
     :recv recv}))

(let [sockets (atom {})
      channels (atom {})]
  (defn get-channel [location app-id]

    ;; Ensure there's a (promise of a) socket for this location
    (swap! sockets (fn [s]
                     (if (get s location)
                       ;; We already have a (promise of a) socket for this location
                       s

                       ;; We need to create a (promise of a) socket for this location
                       (let [p (promise)]
                         (future (deliver p (open-socket location)))
                         (assoc s location p)))))

    (let [socket-promise (get @sockets location)]

      ;; Ensure there are channels for this app to this location

      (swap! channels (fn [cs]
                        (if (get-in cs [location app-id])

                          ;; We already have a channel
                          cs

                          ;; We need to create a channel
                          (let [to-socket (chan)]
                            (future
                              (let [socket @socket-promise]
                                (println "Initialising channel for app" app-id "...")
                                (Thread/sleep 1000)
                                (println "Done")

                                ;; Send outgoing messages
                                (go-loop [n 1]
                                  (if-let [msg (<! to-socket)]
                                    (do ((:send socket) msg)
                                        (println "Delivered" n "messages")
                                        (recur (inc n)))
                                    (println "Channel closed")))

                                ;; Receive incoming messages
                                (go-loop []
                                  (when-let [reply (<! (:recv socket))]
                                    (do (if-let [request-id (:request-id reply)]
                                          (let [reply-channel (get-reply-channel request-id)]
                                            (if (offer! reply-channel reply)
                                              (println "Received reply msg" (:result reply))))
                                          (println "Received non-reply message:" reply))
                                        (recur))))))
                            (assoc-in cs [location app-id] to-socket)))))
      (get-in @channels [location app-id]))))

(let [next-request-id (atom 0)]
  (defn req-rep [location app-id msg]
    (let [out (get-channel location app-id)
          rid (swap! next-request-id inc)]
      (go
        (>! out {:request-id rid
                 :request msg})
        (alt!
          (get-reply-channel rid) ([reply] (println "RESULT" rid ":" (:result reply)))
          (timeout 500) (println "Timeout" rid))

        (clear-reply-channel rid)))))

(defn send-thing [location app-id]
  (doseq [x (range 10)]
    (req-rep "localhost" 456 (str "This is message " x))))