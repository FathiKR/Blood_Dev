import pandas as pd
import streamlit as st
import os
import plotly.express as px
from numerize.numerize import numerize
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Malaysia Blood Donation", page_icon=":drop_of_blood:", layout="wide")

st.subheader("💉🩸 Malaysia Blood Donation Overview")

hospital_path = "df_facility_master.parquet.gzip"

df_hospital_master = pd.read_parquet(hospital_path)

st.sidebar.header("Please filter")

#Filter for hospital
hospital = st.sidebar.selectbox(
    label="Select Hospital",
    options = df_hospital_master['hospital'].unique(),
    # index = df_hospital_master["hospital"] == "Pusat Darah Negara"
)

year = st.sidebar.multiselect(
    label="Select Year",
    options=df_hospital_master["year"].unique(),
    default=df_hospital_master["year"].unique()

)
month = st.sidebar.multiselect(
    label="Select Month",
    options=df_hospital_master["month"].unique(),
    default=df_hospital_master["month"].unique()
)
df_hospital_selection = df_hospital_master.query(
    "hospital==@hospital & year==@year & month==@month"
)
col1, col2, col3 = st.columns([1,1,1])

with col1:
    # cont = st.container(border = True, height = 300)
    col1.title("Total Donation")
    total_donation = float(df_hospital_selection["daily"].sum())
    col1.metric(label="Total Donation", value= numerize(total_donation))

with col2:
    col2.title("Blood A")
    total_a = float(df_hospital_selection["blood_a"].sum())
    col2.metric(label="Total Blood A", value= numerize(total_a))

with col2:
    col2.title("Blood AB")
    total_ab = float(df_hospital_selection["blood_ab"].sum())
    col2.metric(label="Total Blood AB", value= numerize(total_ab))

with col3:
    col3.title("Blood B")
    total_a = float(df_hospital_selection["blood_b"].sum())
    col3.metric(label="Total Blood B", value= numerize(total_a))
    
with col3:
    col3.title("Blood O")
    total_o = float(df_hospital_selection["blood_o"].sum())
    col3.metric(label="Total Blood O", value= numerize(total_o))

cont_trend = st.container(border = True, height =600)
cont_trend.title("Blood Donation Trend")
fig = px.line(df_hospital_selection, x='date', y="daily",labels={
                     "date": "Date",
                     "daily": "Total Donation"})
cont_trend.plotly_chart(fig, use_container_width=True, height = 500)


bar_col, stack_col, regular_col = st.columns(3)

hospital_social = df_hospital_selection.groupby(by="hospital")
df_hospital_social = pd.DataFrame(
        {
            "social_civilian":hospital_social["social_civilian"].sum(),
            "social_student":hospital_social["social_student"].sum(),
            "social_policearmy":hospital_social["social_policearmy"].sum()
        }).reset_index()

with bar_col:
    cont = bar_col.container(border = True, height = 600)    
    cont.title("Social Group")
    fig = px.bar(df_hospital_social, x="hospital", y=["social_civilian", "social_student", "social_policearmy"])
    cont.plotly_chart(fig,height = 550)
    cont.dataframe(df_hospital_social)

hospital_age = df_hospital_selection.groupby(by="hospital")

df_hospital_age = pd.DataFrame(
        {
            "17-24":hospital_age["17-24"].sum(),
            "25-29":hospital_age["25-29"].sum(),
            "30-34":hospital_age["30-34"].sum(),
            "35-39":hospital_age["35-39"].sum(),
            "40-44":hospital_age["40-44"].sum(),
            "45-49":hospital_age["45-49"].sum(),
            "50-54":hospital_age["50-54"].sum(),
            "55-59":hospital_age["55-59"].sum(),
            "60-64":hospital_age["60-64"].sum(),
            "other":hospital_age["other"].sum()
        }).reset_index()

with stack_col:
    cont = stack_col.container(border = True, height = 600)    
    cont.title("Age Group")
    fig = px.bar(df_hospital_age, x="hospital", y=["17-24","25-29","30-34",	"35-39","40-44","45-49","50-54","55-59","60-64","other"])
    cont.plotly_chart(fig,height = 550)
    cont.dataframe(df_hospital_age)

hospital_regular = df_hospital_selection.groupby(by="hospital")

df_hospital_regular = pd.DataFrame(
        {
            "donations_new":hospital_regular["donations_new"].sum(),
            "donations_regular":hospital_regular["donations_regular"].sum(),
            "donations_irregular":hospital_regular["donations_irregular"].sum()
        }).reset_index()

with regular_col:
    cont = regular_col.container(border = True, height = 600)    
    cont.title("Donation Category")
    fig = px.bar(df_hospital_regular, x="hospital", y=["donations_new","donations_regular","donations_irregular"])
    cont.plotly_chart(fig,height = 550)
    cont.dataframe(df_hospital_regular)

df_hospital_filter = df_hospital_master.query(
    "year==@year & month==@month"
)

hospital_col, hospital_table = st.columns(2)

df_hospital_rank = df_hospital_filter.groupby("hospital")["daily"].sum().reset_index()
df_hospital_rank = df_hospital_rank.sort_values(by="daily", ascending = False).reset_index(drop=True)

with hospital_col:
    cont = hospital_col.container(border=True, height = 600)
    cont.title("Donation Ranking By hospital")
    fig = px.bar(df_hospital_rank, x="daily", y="hospital", orientation='h')
    cont.plotly_chart(fig, use_container_width=True, height = 500)

with hospital_table:
    cont = hospital_table.container(border=True, height = 600)
    cont.title("Donation Ranking By hospital")
    cont.dataframe(df_hospital_rank,use_container_width=True)