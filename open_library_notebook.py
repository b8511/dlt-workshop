import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import dlt
    import plotly.express as px
    return dlt, mo, px


@app.cell
def _(dlt):
    # Connect to the open_library_pipeline dataset via ibis
    pipeline = dlt.pipeline(pipeline_name="open_library_pipeline", destination="duckdb")
    dataset = pipeline.dataset()
    dataset_name = pipeline.dataset_name
    ibis_connection = dataset.ibis()
    return dataset_name, ibis_connection


@app.cell
def _(dataset_name, ibis_connection):
    # Query top 10 authors by book count using ibis
    author_table = ibis_connection.table("books_search__author_name", database=dataset_name)
    import ibis
    top_authors = (
        author_table
        .group_by("value")
        .agg(book_count=author_table.value.count())
        .order_by(ibis.desc("book_count"))
        .limit(10)
    )
    top_authors_df = top_authors.execute()
    top_authors_df = top_authors_df.rename(columns={"value": "author_name"})
    top_authors_df
    return (top_authors_df,)


@app.cell
def _(mo, px, top_authors_df):
    # Visualize top 10 authors by book count
    fig = px.bar(
        top_authors_df.sort_values("book_count", ascending=True),
        x="book_count",
        y="author_name",
        orientation="h",
        title="Top 10 Authors by Book Count (Harry Potter Search)",
        labels={"book_count": "Number of Books", "author_name": "Author"},
        color="book_count",
        color_continuous_scale="Viridis",
    )
    fig.update_layout(yaxis=dict(categoryorder="total ascending"), height=500)
    mo.ui.plotly(fig)
    return


if __name__ == "__main__":
    app.run()
