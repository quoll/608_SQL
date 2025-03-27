#!/usr/bin/env bb
(require '[clojure.string :as str])
(require '[clojure.java.io :as io])

(def input-file "../data/shopping_trends.csv")
(def output-file "../data/data.sql")

(def purchase-columns [0 5 9 10 12 13 14 15])
(def item-columns [3 4 7 8])
(def customer-columns [0 1 2 6 11 16 17 18])
(def payment-columns [12 17])
(def category-column 4)
(def location-column 6)
(def season-column 9)
(def shipping-column 13)
(def frequency-column 18)

(def item-strings [1 3 4])
(def customer-strings [2 4])
(def purchase-strings [8 9])

;; foreign keys, by the final column index
(def item-fks [2])
(def customer-fks [3 6 7])
(def purchase-fks [4 6 7]) ;; also 1 2, but these are accounted for

(defn index-of [v coll]
  (first (keep-indexed #(when (= v %2) %1) coll)))

(def preferred-payment-method (index-of 17 customer-columns))
(def payment-method (index-of 12 purchase-columns))

(def location (index-of location-column customer-columns))
(def frequency (index-of frequency-column customer-columns))
(def season (index-of season-column purchase-columns))
(def shipping-type (index-of shipping-column purchase-columns))
;; increment to deal with the inserted index column
(def item-category (inc (index-of category-column item-columns)))

(defn load-data [filename]
  (->> (slurp filename)
       (str/split-lines)
       (map #(vec (str/split % #",")))))

(defn prepend [& args]
  (into (vec (butlast args)) (last args)))

(defn select [columns row]
  (mapv #(nth row %) columns))

(defn cut-f [columns data]
  (map #(select columns %) data))

(defn wrap-strings [data columns]
  (reduce (fn [result i] (update result i #(str \' % \'))) data columns))

(defn line
  ([data columns] (line data columns false))
  ([data columns first-line]
   (str (if first-line "\n    (" ",\n    (") (str/join "," (wrap-strings data columns)) ")")))

(defn fks [data columns]
  (reduce #(update %1 %2 str "_id") data columns))

;; Load the data
(def shopping (load-data input-file))
;; (def shopping-headers (first shopping))
(def shopping-headers ["customer_id" "age" "gender" "item_purchased" "category" "purchase_amount" "location" "size" "color" "season" "review_rating" "subscription_status" "payment_method" "shipping_type" "discount_applied" "promo_code_used" "previous_purchases" "preferred_payment_method" "frequency_of_purchases"])
(def shopping-data (rest shopping))

(with-open [w (io/writer output-file)]
  (binding [*out* w]

    ;; Extract the Payment Methods
    (let [[fmethod & payment-methods :as all-payment-methods] (->> (cut-f payment-columns shopping-data)
                                                                   flatten
                                                                   set
                                                                   sort
                                                                   (map-indexed (fn [i x] [i x])))]
      (print (str "INSERT INTO payment_method ("
                  (str/join "," (prepend "payment_method_id" (select (take 1 payment-columns) shopping-headers)))
                  ") VALUES"))
      (print (line fmethod [1] true))
      (doseq [method payment-methods]
        (print (line method [1])))
      (println ";\n")
      (def payment-map (into {} (map (juxt second first) all-payment-methods))))

    ;; Extract the Location names
    (let [[flocation & locations :as all-locations] (->> (cut-f [location-column] shopping-data)
                                                         flatten
                                                         set
                                                         sort
                                                         (map-indexed (fn [i x] [i x])))]
      (print "INSERT INTO location (location_id, location_name) VALUES")
      (print (line flocation [1] true))
      (doseq [loc locations]
        (print (line loc [1])))
      (println ";\n")
      (def location-map (into {} (map (juxt second first) all-locations))))

    ;; Extract the Frequency names
    (let [[ffreq & freqs :as all-freqs] (->> (cut-f [frequency-column] shopping-data)
                                             flatten
                                             set
                                             sort
                                             (map-indexed (fn [i x] [i x])))]
      (print "INSERT INTO frequency (frequency_id, frequency_name) VALUES")
      (print (line ffreq [1] true))
      (doseq [freq freqs]
        (print (line freq [1])))
      (println ";\n")
      (def freq-map (into {} (map (juxt second first) all-freqs))))

    ;; Extract the Season names
    (let [[fseason & seasons :as all-seasons] (->> (cut-f [season-column] shopping-data)
                                                   flatten
                                                   set
                                                   sort
                                                   (map-indexed (fn [i x] [i x])))]
      (print "INSERT INTO season (season_id, season_name) VALUES")
      (print (line fseason [1] true))
      (doseq [season seasons]
        (print (line season [1])))
      (println ";\n")
      (def season-map (into {} (map (juxt second first) all-seasons))))

    ;; Extract the Shipping names
    (let [[fshipping & shippings :as all-shippings] (->> (cut-f [shipping-column] shopping-data)
                                                         flatten
                                                         set
                                                         sort
                                                         (map-indexed (fn [i x] [i x])))]
      (print "INSERT INTO shipping_type (shipping_type_id, shipping_type_name) VALUES")
      (print (line fshipping [1] true))
      (doseq [shipping shippings]
        (print (line shipping [1])))
      (println ";\n")
      (def shipping-map (into {} (map (juxt second first) all-shippings))))

    ;; Extract the Category names
    (let [[fcategory & categories :as all-categories] (->> (cut-f [category-column] shopping-data)
                                                           flatten
                                                           set
                                                           sort
                                                           (map-indexed (fn [i x] [i x])))]
      (print "INSERT INTO category (category_id, category_name) VALUES")
      (print (line fcategory [1] true))
      (doseq [category categories]
        (print (line category [1])))
      (println ";\n")
      (def category-map (into {} (map (juxt second first) all-categories))))

    ;; Extract the Items
    (let [[fitem & items :as all-items] (->> (cut-f item-columns shopping-data)
                                             set
                                             sort
                                             (map-indexed (fn [i data] (into [i] data))))]
      (print (str "INSERT INTO item ("
                  (str/join "," (fks (prepend "item_id" (select item-columns shopping-headers)) item-fks))
                  ") VALUES"))
      (print (line (update fitem item-category category-map) item-strings true))
      (doseq [item items]
        (let [pitem (update item item-category category-map)]
          (print (line pitem item-strings))))
      (println ";\n")
      (def item-map (into {} (map (juxt rest first) all-items))))

    (let [[fshop & rshopping-data] shopping-data]
      ;; Extract the Customers
      (print (str "INSERT INTO customer ("
                  (str/join "," (fks (select customer-columns shopping-headers) customer-fks))
                  ") VALUES"))
      (let [pcust (fn [s] (-> (select customer-columns s)
                              ;; update the string for the preferred payment method with the index of the method
                              (update preferred-payment-method payment-map)
                              (update frequency freq-map)
                              (update location location-map)))]
        (print (line (pcust fshop) customer-strings true))
        (doseq [row rshopping-data]
          (print (line (pcust row) customer-strings))))
      (println ";\n")

      ;; Extract the Purchases
      (print (str "INSERT INTO purchase ("
                  (str/join "," (fks (prepend "purchase_id" "item_id"
                                              (select purchase-columns shopping-headers)) purchase-fks))
                  ") VALUES"))
      (let [ppurch (fn [s]
                     (let [customer-id (first s)
                           item-id (get item-map (select item-columns s))
                           purchase (select purchase-columns s)
                           ;; update the string for the payment method with the index of the method
                           ppurchase (-> purchase
                                         (update payment-method payment-map)
                                         (update season season-map)
                                         (update shipping-type shipping-map))]
                       (prepend customer-id item-id ppurchase)))]
        (print (line (ppurch fshop) purchase-strings true))
        (doseq [row rshopping-data]
          (print (line (ppurch row) purchase-strings))))
      (println ";\n"))))

