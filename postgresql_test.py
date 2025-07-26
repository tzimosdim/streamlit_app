import asyncio
import pandas as pd
from psqlpy import ConnectionPool
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st

# 🎯 Replace these with your Supabase connection details
DB_HOST = "db.lhcdfkxamzjrrtlaqbzf.supabase.co"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "nlnrFID8TalA5rjn"


async def list_tables():
    pool = ConnectionPool(
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        db_name=DB_NAME,
        max_db_pool_size=5
    )

    async with pool.acquire() as conn:
        res = await conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
        )
        rows = res.result()

    pool.close()
    return rows


tables = asyncio.run(list_tables())
print("Tables in public schema:")
for t in tables:
    print(" -", t)


async def list_data():
    pool = ConnectionPool(
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        db_name=DB_NAME,
        max_db_pool_size=5
    )

    async with pool.acquire() as conn:
        query = """
                SELECT 
                    p.product_id,
                    p.product_name,
                    SUM(od.quantity) AS total_quantity_sold,
                    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS total_revenue
                FROM 
                    order_details od
                JOIN 
                    products p ON od.product_id = p.product_id
                GROUP BY 
                    p.product_id, p.product_name
                ORDER BY 
                    total_revenue DESC, total_quantity_sold DESC
                LIMIT 10;
                """

        result = await conn.execute(query)
        data = result.result()
        df = pd.DataFrame(data)

    pool.close()
    return df

def plot_top_products_by_quantity(df, top_n=10):
    """
    Plots a bar chart of top products by quantity sold.

    Parameters:
    df (pd.DataFrame): DataFrame with at least 'product_name' and 'total_quantity_sold' columns
    top_n (int): Number of top products to display

    Returns:
    matplotlib.figure.Figure: Bar chart figure
    """
    # Sort and select top N
    top_df = df.sort_values(by='total_quantity_sold', ascending=False).head(top_n)

    # Set plot style
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))

    # Create barplot
    ax = sns.barplot(
        x='product_name',
        y='total_quantity_sold',
        data=top_df,
        palette='viridis'
    )

    # Customize plot
    ax.set_title(f"Top {top_n} Products by Quantity Sold", fontsize=16)
    ax.set_xlabel("Product Name", fontsize=12)
    ax.set_ylabel("Total Quantity Sold", fontsize=12)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    plt.tight_layout()

    plt.show()



data = asyncio.run(list_data())
print(data)
# plot_top_products_by_quantity(data)



def show_top_products_chart(df: pd.DataFrame, top_n: int = 10):
    """
    Displays a bar chart of top-selling products by quantity using Streamlit.

    Parameters:
    df (pd.DataFrame): DataFrame with 'product_name' and 'total_quantity_sold'
    top_n (int): Number of top products to show
    """
    # Validate input
    if 'product_name' not in df.columns or 'total_quantity_sold' not in df.columns:
        st.error("The DataFrame must contain 'product_name' and 'total_quantity_sold' columns.")
        return

    # Sort and filter
    top_df = df.sort_values(by='total_quantity_sold', ascending=False).head(top_n)

    # Plot
    st.subheader(f"Top {top_n} Products by Quantity Sold")
    sns.set(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(
        x='product_name',
        y='total_quantity_sold',
        data=top_df,
        palette='viridis',
        ax=ax
    )
    ax.set_title("Top Selling Products", fontsize=16)
    ax.set_xlabel("Product Name")
    ax.set_ylabel("Total Quantity Sold")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    st.pyplot(fig)


# Example Streamlit usage
st.set_page_config(page_title="Top Selling Products", layout="wide")
st.title("📊 Top Selling Products Viewer")


# Let user choose number of products
top_n = st.slider("Select number of top products to display", min_value=3, max_value=30, value=10)

# Show chart
show_top_products_chart(data, top_n=top_n)
