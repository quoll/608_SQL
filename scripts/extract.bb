#!/usr/bin/env bb
(require '[clojure.string :as str])
(require '[clojure.java.io :as io])

(def input-file "shopping_trends.csv")
(def item-file "item.csv")
(def category-file "category.csv")
(def purchase-file "purchase.csv")
(def customer-file "customer.csv")
(def payment-file "payment_method.csv")
(def location-file "location.csv")
(def season-file "season.csv")
(def shipping-type-file "shipping_type.csv")
(def frequency-file "frequency.csv")

(def purchase-columns [0 5 9 10 12 13 14 15])
(def item-columns [3 4 7 8])
(def customer-columns [0 1 2 6 11 16 17 18])
(def payment-columns [12 17])
(def category-column 4)
(def location-column 6)
(def season-column 9)
(def shipping-column 13)
(def frequency-column 18)

(defn index-of [v coll]
  (first (keep-indexed #(when (= v %2) %1) coll)))

(def preferred-payment-method (index-of 17 customer-columns))
(def payment-method (index-of 12 purchase-columns))

(def location (index-of location-column customer-columns))
(def frequency (index-of frequency-column customer-columns))
(def season (index-of season-column purchase-columns))
(def shipping-type (index-of shipping-column purchase-columns))
;; increment to deal with the inserted index column
(def category (inc (index-of category-column item-columns)))

(defn load-data [filename]
  (->> (slurp filename)
       (str/split-lines)
       (map #(vec (str/split % #",")))))

(defn prepend [& args]
  (concat (butlast args) (last args)))

(defn select [columns row]
  (mapv #(nth row %) columns))

(defn cut-f [columns data]
  (map #(select columns %) data))

;; Load the data
(def shopping (load-data "shopping_trends.csv"))
(def shopping-headers (first shopping))
(def shopping-data (rest shopping))

;; Extract the Payment Methods
(def payment-methods (->> (cut-f payment-columns shopping-data) flatten set sort (map-indexed (fn [i x] [(inc i) x]))))
(with-open [w (io/writer payment-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select (take 1 payment-columns) shopping-headers))))
    (doseq [method payment-methods]
      (println (str/join "," method)))))
(def payment-map (into {} (map (juxt second first) payment-methods)))

;; Extract the Location names
(def locations (->> (cut-f [location-column] shopping-data) flatten set sort (map-indexed (fn [i x] [(inc i) x]))))
(with-open [w (io/writer location-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select [location-column] shopping-headers))))
    (doseq [loc locations]
      (println (str/join "," loc)))))
(def location-map (into {} (map (juxt second first) locations)))

;; Extract the Frequency names
(def freqs (->> (cut-f [frequency-column] shopping-data) flatten set sort (map-indexed (fn [i x] [(inc i) x]))))
(with-open [w (io/writer frequency-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select [frequency-column] shopping-headers))))
    (doseq [freq freqs]
      (println (str/join "," freq)))))
(def freq-map (into {} (map (juxt second first) freqs)))

;; Extract the Season names
(def seasons (->> (cut-f [season-column] shopping-data) flatten set sort (map-indexed (fn [i x] [(inc i) x]))))
(with-open [w (io/writer season-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select [season-column] shopping-headers))))
    (doseq [season seasons]
      (println (str/join "," season)))))
(def season-map (into {} (map (juxt second first) seasons)))

;; Extract the Shipping names
(def shippings (->> (cut-f [shipping-column] shopping-data) flatten set sort (map-indexed (fn [i x] [(inc i) x]))))
(with-open [w (io/writer shipping-type-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select [shipping-column] shopping-headers))))
    (doseq [shipping shippings]
      (println (str/join "," shipping)))))
(def shipping-map (into {} (map (juxt second first) shippings)))

;; Extract the Category names
(def categories (->> (cut-f [category-column] shopping-data) flatten set sort (map-indexed (fn [i x] [(inc i) x]))))
(with-open [w (io/writer category-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select [category-column] shopping-headers))))
    (doseq [category categories]
      (println (str/join "," category)))))
(def category-map (into {} (map (juxt second first) categories)))

;; Extract the Items
(def items (->> (cut-f item-columns shopping-data) set sort (map-indexed (fn [i data] (into [(inc i)] data)))))
(with-open [w (io/writer item-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "id" (select item-columns shopping-headers))))
    (doseq [item items]
      (let [pitem (-> item
                      (update (inc category) category-map))]
        (println (str/join "," pitem))))))
(def item-map (into {} (map (juxt rest first) items)))

;; Extract the Customers
(with-open [w (io/writer customer-file)]
  (binding [*out* w]
    (println (str/join "," (select customer-columns shopping-headers)))
    (doseq [row shopping-data]
      (let [customer (select customer-columns row)
            ;; update the string for the preferred payment method with the index of the method
            pcustomer (-> customer
                          (update preferred-payment-method payment-map)
                          (update frequency freq-map)
                          (update location location-map))]
        (println (str/join "," pcustomer))))))

;; Extract the Purchases
(with-open [w (io/writer purchase-file)]
  (binding [*out* w]
    (println (str/join "," (prepend "purchase_id" "item_id" (select purchase-columns shopping-headers))))
    (doseq [row shopping-data]
      (let [customer-id (first row)
            item-id (get item-map (select item-columns row))
            purchase (select purchase-columns row)
            ;; update the string for the payment method with the index of the method
            ppurchase (-> purchase
                          (update payment-method payment-map)
                          (update season season-map)
                          (update shipping-type shipping-map))]
        (println (str/join "," (prepend customer-id item-id ppurchase)))))))

