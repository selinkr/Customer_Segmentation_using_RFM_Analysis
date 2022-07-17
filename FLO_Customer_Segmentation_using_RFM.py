import datetime as dt
import pandas as pd

df_ = pd.read_csv("CRM_Analytics/Projects/Datasets/flo_data_20k.csv")
df = df_.copy()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# Understanding and Preparing the Data

df.head(10) # First 10 observations
df.isnull().sum() # Null values
df.describe().T # Descriptive statistics
df.info() # Examination of variable types
df.shape # Shape of the dataset
df.columns # Name of the variables

df["Order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["last_order_date"])
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])

df.groupby("order_channel").agg({"master_id":"count",
                                 "Order_num_total":"sum",
                                "customer_value_total":"sum"})

df.sort_values("customer_value_total", ascending=False)[0:10]
df.sort_values("Order_num_total", ascending=False)[0:10]

def data_preparation(dataframe):
    dataframe["Order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_online"] + dataframe["customer_value_total_ever_offline"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime())
    return df

df["last_order_date"].max() #2021-05-30
today_date = dt.datetime(2021, 6, 1)

# Calculating RFM Metrics

RFM = pd.DataFrame()
RFM["customer_id"] = df["master_id"]
RFM["Recency"] = (today_date - df["last_order_date"]).astype('timedelta64[D]')
RFM["Frequency"] = df["Order_num_total"]
RFM["Monetary"] = df["customer_value_total"]

RFM.head()

# Calculating the RF Score

RFM["recency_score"] = pd.qcut(RFM['Recency'], 5, labels=[5, 4, 3, 2, 1])
RFM["frequency_score"] = pd.qcut(RFM['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
RFM["monetary_score"] = pd.qcut(RFM['Monetary'], 5, labels=[1, 2, 3, 4, 5])

RFM["RF_SCORE"] = (RFM["recency_score"].astype(str) + RFM["frequency_score"].astype(str))

# Converting RF Score to the Segment Form

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

RFM['segment'] = RFM['RF_SCORE'].replace(seg_map, regex=True)

RFM[["segment", "Recency", "Frequency", "Monetary"]].groupby("segment").agg(["mean", "count"])

# Testing with Cases
"""Case 1: FLO includes a new women's shoe brand. The product prices of the new brand  are above the general 
   customer preferences. For this reason, it is desired to contact the customers in the profile that will be interested 
   in the promotion of the brand and product sales.
   Those who are in Champions, loyal_customers segments and female category are the customers to be focused."""

RFM["interested_in_categories"] = df["interested_in_categories_12"]
new_brand_target_customer_ids = RFM.loc[((RFM["segment"] == "champions") | (RFM["segment"] == "loyal_customers")) & (RFM["interested_in_categories"].str.contains("KADIN")), "customer_id"]

new_brand_target_customer_ids.head()

new_brand_target_customer_ids.shape

new_brand_target_customer_ids.to_csv("new_brand_target_customer_id.csv", index=False)

"""Case 2: Nearly 40% discount is planned for Men's and Children's products.It is aimed to specifically target customers
 who are good customers in the past, but who have not shopped for a long time, who are interested in the categories 
 related to this discount, who should not be lost, who are asleep and new customers."""

target_segments_for_discount40_customer_ids = RFM[RFM["segment"].isin(["cant_loose", "hibernating", "new_customers"])]["customer_id"]

discount_customer_ids = df[(df["master_id"].isin(target_segments_for_discount40_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]

discount_customer_ids.head()

discount_customer_ids.to_csv("discount_target_customer_id.csv", index=False)

""" FUNCTIONALIZING 

def rfm_analysis(dataframe):
    dataframe["Order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_online"] + dataframe["customer_value_total_ever_offline"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime())

    dataframe["last_order_date"].max()
    today_date = dataframe.datetime(2021, 6, 1)

    RFM = pd.DataFrame()
    RFM["customer_id"] = dataframe["master_id"]
    RFM["Recency"] = (today_date - dataframe["last_order_date"]).astype('timedelta64[D]')
    RFM["Frequency"] = dataframe["Order_num_total"]
    RFM["Monetary"] = dataframe["customer_value_total"]

    RFM.head()

    RFM["recency_score"] = pd.qcut(RFM['Recency'], 5, labels=[5, 4, 3, 2, 1])
    RFM["frequency_score"] = pd.qcut(RFM['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    RFM["monetary_score"] = pd.qcut(RFM['Monetary'], 5, labels=[1, 2, 3, 4, 5])

    RFM["RF_SCORE"] = (RFM["recency_score"].astype(str) + RFM["frequency_score"].astype(str))

    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    RFM['segment'] = RFM['RF_SCORE'].replace(seg_map, regex=True)
    return RFM[["customer_id", "Recency", "Frequency", "Monetary", "RF_SCORE", "segment"]] """