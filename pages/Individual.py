import pandas as pd
import streamlit as st
import os
import plotly.express as px
from numerize.numerize import numerize
import warnings
import plotly.figure_factory as ff
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Malaysia Blood Donation", page_icon=":drop_of_blood:", layout="wide")

st.subheader("💉🩸 Malaysia Blood Donation Overview")

individual_path = "df_retain_master.parquet.gzip"

df_individual_master = pd.read_parquet(individual_path)

col1, col2, col3 = st.columns([1,1,1])

with col1:
    # cont = st.container(border = True, height = 300)
    col1.title("Total Donors")
    total_donation = int(df_individual_master["donor_id"].count())
    col1.metric(label="Total Donors", value= numerize(total_donation))

with col2:
    # cont = st.container(border = True, height = 300)
    col2.title("Total Donation")
    total_donation = int(df_individual_master["total_donation"].sum())
    col2.metric(label="Total Donation", value= numerize(total_donation))

rankchart_col, ranktable_col = st.columns([1,1])
# df_individual_master = df_individual_master.sort_values("total_donation", ascending=True, ignore_index=True)
df_individual_top10 = df_individual_master.nlargest(10,"total_donation")
df_individual_top10 = df_individual_master.nlargest(10,"total_donation").sort_values("total_donation", ascending=True, ignore_index=True)

with rankchart_col:

    cont_chart = rankchart_col.container(border=True, height=600)
    cont_chart.title("Top 10 Blood's Donor")
    fig = px.bar(df_individual_top10, x="total_donation", y="donor_id", orientation='h')
    cont_chart.plotly_chart(fig, use_container_width=True, height = 300)

with ranktable_col:
    df_individual_top10 = df_individual_master.nlargest(10,"total_donation").sort_values("total_donation", ascending=False, ignore_index=True)
    cont_table = ranktable_col.container(border=True, height=600)
    cont_table.title("Blood's Donor Info")
    cont_table.table(df_individual_top10)
    # cont_table.plotly_chart(fig, use_container_width=True, height = 300)


container_scatter = st.container(border=True, height = 500)
container_scatter.title("Age Donor Vs Frequency Donation")
fig = px.scatter(df_individual_master, x="last_donate_age", y="total_donation")
container_scatter.plotly_chart(fig, use_container_width=True, height=450)


container_scatter2 = st.container(border=True, height = 600)
container_scatter2.title("Age Donate Vs Donation Interval")
# fig = ff.create_distplot([df_individual_master["last_donate_agge"], df_individual_master["median_frequency_donor_months"]], group_labels, bin_size=.50,
#                          curve_type='normal' # override default 'kde'
#                         )
fig = px.histogram(df_individual_master, x="last_donate_age", y="median_frequency_donor_months",hover_data=df_individual_master.columns, histfunc="count",histnorm="density",nbins=100)
container_scatter2.plotly_chart(fig, use_container_width=True, height=500)


