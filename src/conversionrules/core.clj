(ns conversionrules.core
  (:require [clojure.data.json :as json]
            [com.rpl.specter :as sp]
            [clojure.pprint :as pp]))

(defn read-and-parse [f]
  (-> f (slurp) (json/read-str :key-fn keyword)))

(defn catalog [f]
  (read-and-parse f))

(defn conversionrules [f]
  (read-and-parse f))

(defn make-conversion-rule [technical-name entitlement-rule src-metric dst-metric cc-model-id]
  (let [src-metric (or src-metric dst-metric)]
    {:id (.toString (java.util.UUID/randomUUID))
     :description (str "conversion of " technical-name "'s `"
                       (or src-metric dst-metric)
                       "` metric to the business metric `"
                       dst-metric "`")
     :entitlementRuleIds [(:id entitlement-rule)]
     :sources [{:metric src-metric}]
     :destination [{:metric dst-metric}]
     :ccModelId cc-model-id}))

(def my-catalog (catalog "/Users/i303874/dev/cmt-prod/cmt-cis-htp740461067-2018-03-26-14-39-28.json"))
(def my-conversionrules (conversionrules "/Users/i303874/SAP SE/Heinrich, Johannes - CP Marketplace/Concepts/Conversion Rules/usageMetadata.json"))
(def sdf (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss"))

(defn legacy-conversion-rules [conversionRules targetProductId targetMetric]
  (filter (fn [{:keys [productId metricId] :as conversionRule}]
            (when (and (= productId targetProductId) (.equalsIgnoreCase metricId targetMetric))
              conversionRule))
          (concat (get-in conversionRules [:products :neo])
                  (get-in conversionRules [:products :cf]))))

(defn collect-metrics [ratePlans]
  (set (sp/select [(sp/walker (fn [n] (and (map? n) (contains? n :metricId)))) :metricId]
                  ratePlans)))

(defn fail-on-multiple-metrics [entitlementRule metrics]
  (when (> (count metrics) 1)
    (throw (ex-info "Different metrics are used in rate plans" {:metrics metrics
                                                                :entitlementRule entitlementRule})))
  metrics)

(defn fail-on-no-metric-return-first [entitlementRule metrics]
  (let [metric (first metrics)]
    (when (nil? metric)
      (throw (ex-info "There are no metrics" {:metrics metrics
                                              :entitlementRule entitlementRule})))
    metric))

(defn composedId->str [{:keys [id catalog version]}]
  (str id "-" catalog "-" version))

(defn lookup-technicalAsset [technicalAssets targetId]
  (some (fn [{:keys [composedId technicalName] :as technicalAsset}]
          (when (= composedId targetId)
            technicalAsset))
        technicalAssets))

(defn after-date? [a b]
  (cond
    (and (nil? a) (nil? b)) a
    (nil? a) b
    (nil? b) a
    :else (.after a b)))

(defn activeRatePlans [ratePlans]
  (reduce (fn [{agroup :group :as a} {bgroup :group :as b}]
            (let [avalidFrom (-> sdf (.parse (:validFrom agroup)))
                  bvalidFrom (-> sdf (.parse (:validFrom bgroup)))]
              (if (after-date? avalidFrom bvalidFrom)
                b a)))
          (or (first ratePlans) {}) ratePlans))

(defn make-metric-conversion-rules [c lookup-legacy-conversion-rules]
  (reduce (fn [ret {:keys [entitlementRules composedId] :as product}]
            (update ret :metricConversionRules concat
                    (apply concat
                           (map (fn [{:keys [technicalAssetId] :as entitlementRule}]
                                  (apply concat
                                         (map (fn [metric]
                                                (let [technicalName (:technicalName (lookup-technicalAsset (:technicalAssets c) (:composedId technicalAssetId)))
                                                      conversion-rules (lookup-legacy-conversion-rules technicalName metric)]
                                                  (condp = (count conversion-rules)
                                                    0 (vector "No conversion rules could be found" {:id (composedId->str composedId)
                                                                                                    :technicalName technicalName
                                                                                                    :metric metric})
                                                    #_1 (map (fn [{:keys [metricNameRawData]}]
                                                               (make-conversion-rule technicalName entitlementRule metricNameRawData metric "TODO"))
                                                             conversion-rules)
                                                    #_(vector "Mulitpe conversion rules match" {:id (composedId->str composedId)
                                                                                                :technicalName technicalName
                                                                                                :metric metric
                                                                                                :rules (count conversion-rules)}))))
                                              (collect-metrics (activeRatePlans (:ratePlans entitlementRule)))))) entitlementRules))))
          {:schemaVersion "1.0"} (filter (fn [{:keys [contractTypes]}]
                                           (some (fn [{:keys [code] :as contractType}]
                                                   (when (= code "cloudcredits")
                                                     contractType))
                                                 contractTypes))
                                         (:products c))))

; is search for product id case sensitive?

(comment
  (->> (make-metric-conversion-rules my-catalog (partial legacy-conversion-rules my-conversionrules)) (json/write-str) (spit "/Users/i303874/Desktop/conversionrules.json"))

  (pp/pprint
   (let [{:keys [products technicalAssets]} my-catalog]
     (reduce (fn [ret {:keys [composedId technicalName entitlementRules]}]
               (let [technicalAssetNames (set
                                          (map (fn [{:keys [technicalAssetId]}]
                                                 (:technicalName (lookup-technicalAsset technicalAssets (:composedId technicalAssetId))))
                                               entitlementRules))]
                 (cond->
                  ret
                   (or (empty? technicalAssetNames)
                       (> (count technicalAssetNames) 1)
                       (not= (first technicalAssetNames) technicalName))
                   (assoc (composedId->str composedId)
                          [technicalName technicalAssetNames]))))
             {} products))))

(defn v100->v120 []
  (->>
   (sp/transform [:products sp/ALL :entitlementRules sp/ALL]
                 (fn [{:keys [contractTypes materialNumber revenueCloudProductId] :as entitlementRule}]
                   (let [{:keys [ratePlans]} entitlementRule
                         metrics (collect-metrics ratePlans)
                         materialNumberMappings (map (fn [metric]
                                                       {:materialNumber materialNumber
                                                        :metricId metric
                                                        :type "ISP"})
                                                     metrics)]
                     (try
                       (fail-on-multiple-metrics entitlementRule metrics)
                       (catch Exception e
                         (println e)))

                     (->
                      entitlementRule
                      (assoc :contractTypes contractTypes)
                      (assoc :materialNumberMappings materialNumberMappings)
                      (assoc :rcProductMappings (map (fn [metric]
                                                       {:metricId metric
                                                        :revenueCloudProductId revenueCloudProductId})
                                                     metrics))
                      (assoc :rcRatePlanMappings (map (fn [{:keys [id revenueCloudRatePlanId] :as ratePlan}]
                                                        (let [metric (fail-on-no-metric-return-first entitlementRule
                                                                                                     (fail-on-multiple-metrics entitlementRule
                                                                                                                               (collect-metrics [ratePlan])))]
                                                          {:id id
                                                           :revenueCloudRatePlanId revenueCloudRatePlanId
                                                           :metricId metric}))
                                                      ratePlans)))))
                 my-catalog)
   (json/write-str)
   (spit "/Users/i303874/Desktop/test.json")))
