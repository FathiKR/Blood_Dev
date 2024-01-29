import pandas as pd
import streamlit as st
import os
import plotly.express as px
from numerize.numerize import numerize
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Malaysia Blood Donation", page_icon=":drop_of_blood:", layout="wide")

st.subheader("💉🩸 Malaysia Blood Donation Overview")

state_path = "df_state_master.parquet.gzip"

df_state_master = pd.read_parquet(state_path)

st.sidebar.header("Please filter")

#Filter for State
state = st.sidebar.selectbox(
    label="Select State",
    options = df_state_master['state'].unique(),
    # default = df_state_master["state"] == "Malaysia"
)

year = st.sidebar.multiselect(
    label="Select Year",
    options=df_state_master["year"].unique(),
    default=df_state_master["year"].unique()

)
month = st.sidebar.multiselect(
    label="Select Month",
    options=df_state_master["month"].unique(),
    default=df_state_master["month"].unique()
)
df_state_selection = df_state_master.query(
    "state==@state & year==@year & month==@month"
)
col1, col2, col3 = st.columns([1,1,1])

with col1:
    # cont = st.container(border = True, height = 300)
    col1.title("Total Donation")
    total_donation = float(df_state_selection["daily"].sum())
    col1.metric(label="Total Donation", value= numerize(total_donation))

with col2:
    col2.title("Blood A")
    total_a = float(df_state_selection["blood_a"].sum())
    col2.metric(label="Total Blood A", value= numerize(total_a))

with col2:
    col2.title("Blood AB")
    total_ab = float(df_state_selection["blood_ab"].sum())
    col2.metric(label="Total Blood AB", value= numerize(total_ab))

with col3:
    col3.title("Blood B")
    total_a = float(df_state_selection["blood_b"].sum())
    col3.metric(label="Total Blood B", value= numerize(total_a))
    
with col3:
    col3.title("Blood O")
    total_o = float(df_state_selection["blood_o"].sum())
    col3.metric(label="Total Blood O", value= numerize(total_o))

cont_trend = st.container(border = True, height =600)
cont_trend.title("Blood Donation Trend")
fig = px.line(df_state_selection, x='date', y="daily",labels={
                     "date": "Date",
                     "daily": "Total Donation"})
cont_trend.plotly_chart(fig, use_container_width=True, height = 500)


bar_col, stack_col, regular_col = st.columns(3)

state_social = df_state_selection.groupby(by="state")
df_state_social = pd.DataFrame(
        {
            "social_civilian":state_social["social_civilian"].sum(),
            "social_student":state_social["social_student"].sum(),
            "social_policearmy":state_social["social_policearmy"].sum()
        }).reset_index()

with bar_col:
    cont = bar_col.container(border = True, height = 600)    
    cont.title("Social Group")
    fig = px.bar(df_state_social, x="state", y=["social_civilian", "social_student", "social_policearmy"])
    cont.plotly_chart(fig,height = 550)
    cont.dataframe(df_state_social)

state_age = df_state_selection.groupby(by="state")

df_state_age = pd.DataFrame(
        {
            "17-24":state_age["17-24"].sum(),
            "25-29":state_age["25-29"].sum(),
            "30-34":state_age["30-34"].sum(),
            "35-39":state_age["35-39"].sum(),
            "40-44":state_age["40-44"].sum(),
            "45-49":state_age["45-49"].sum(),
            "50-54":state_age["50-54"].sum(),
            "55-59":state_age["55-59"].sum(),
            "60-64":state_age["60-64"].sum(),
            "other":state_age["other"].sum()
        }).reset_index()

with regular_col:
    cont = stack_col.container(border = True, height = 600)    
    cont.title("Age Group")
    fig = px.bar(df_state_age, x="state", y=["17-24","25-29","30-34","35-39","40-44","45-49","50-54","55-59","60-64","other"])
    cont.plotly_chart(fig,height = 550)
    cont.dataframe(df_state_age)

state_regular = df_state_selection.groupby(by="state")

df_state_regular = pd.DataFrame(
        {
            "donations_new":state_regular["donations_new"].sum(),
            "donations_regular":state_regular["donations_regular"].sum(),
            "donations_irregular":state_regular["donations_irregular"].sum()
        }).reset_index()

with regular_col:
    cont = regular_col.container(border = True, height = 600)    
    cont.title("Donation Category")
    fig = px.bar(df_state_regular, x="state", y=["donations_new","donations_regular","donations_irregular"])
    cont.plotly_chart(fig,height = 550)
    cont.dataframe(df_state_regular)




df_state_filter = df_state_master.query(
    "year==@year & month==@month"
)

state_col, state_table = st.columns(2)

df_state_rank = df_state_filter.groupby("state")["daily"].sum().reset_index()
df_state_rank = df_state_rank.sort_values(by="daily", ascending = False).reset_index(drop=True)

with state_col:
    cont = state_col.container(border=True, height = 600)
    cont.title("Donation Ranking By State")
    fig = px.bar(df_state_rank, x="daily", y="state", orientation='h')
    cont.plotly_chart(fig, use_container_width=True, height = 500)

with state_table:
    cont = state_table.container(border=True, height = 600)
    cont.title("Donation Ranking By State")
    cont.dataframe(df_state_rank,use_container_width=True)
# with pie_col:

#     pie_col.title("Civillian Group")
#     fig = px.pie(df_state_selection, values = "")