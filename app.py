from flask import Flask, request, Response
import pandas as pd
import sqlite3
from jinja2 import Environment, FileSystemLoader

app = Flask(__name__) 

file_loader = FileSystemLoader("templates")
env = Environment(loader=file_loader)
template = env.get_template("docs.html")


@app.route('/albums')
def home():
    conn = sqlite3.connect('data/chinook.db')
    albums = pd.read_sql_query(
        """
            SELECT 
            albums.AlbumId, albums.Title as Album, artists.Name as Artist, tracks.Composer, genres.Name as Genre, tracks.UnitPrice
            FROM tracks
            LEFT JOIN albums ON albums.AlbumId = tracks.AlbumId
            LEFT JOIN artists ON artists.ArtistId = albums.AlbumId
            LEFT JOIN genres ON genres.GenreId = tracks.GenreId
        """
        , conn, index_col="AlbumId")
    albums = albums.drop_duplicates(subset=["Album"], keep="first")

    mask = albums["Artist"] != "None"
    data = albums[mask].to_json()
    res = Response(response = data, status = 200, mimetype="application/json")
    
    return (res)

@app.route('/albums/top/<country>')
def get_topbuy_album(country):
    conn = sqlite3.connect('data/chinook.db')
    top_albums = pd.read_sql_query(
        """
            SELECT 
            albums.AlbumId, albums.Title as Album, artists.Name as Artist, tracks.Composer, invoices.BillingCountry as Country,
            genres.Name as Genre, tracks.UnitPrice, invoices.Total
            FROM tracks
            LEFT JOIN albums ON albums.AlbumId = tracks.AlbumId
            LEFT JOIN artists ON artists.ArtistId = albums.AlbumId
            LEFT JOIN genres ON genres.GenreId = tracks.GenreId
            LEFT JOIN invoice_items ON invoice_items.InvoiceLineId = tracks.TrackId
            LEFT JOIN invoices ON invoices.InvoiceId = invoice_items.InvoiceLineId
        """, conn)
    top_albums[["Country", "Genre"]] = top_albums[["Country", "Genre"]].astype("category", errors="raise")
    top_albums["Country"] = top_albums["Country"].str.lower()
    top_albums = top_albums.groupby(["Country", "Album"])[["Total"]].agg("count").sort_values("Total", ascending=False).reset_index([0, 1])

    mask = top_albums["Country"] == country
    data = top_albums[mask].dropna().to_json()

    res = Response(response = data, status = 200, mimetype="application/json")

    return (res)


@app.route('/country')
def get_country():
    conn = sqlite3.connect('data/chinook.db')
    country = pd.read_sql_query(
        """
            SELECT customers.Country FROM customers
        """, conn
    )

    country["Country"] = pd.DataFrame(country["Country"].unique())
    data = country.dropna().to_json()

    res = Response(response = data, status = 200, mimetype="application/json")

    return (res)

@app.route('/invoices/total/<year>')
def total_invoices(year):
    year = int(year)
    conn = sqlite3.connect('data/chinook.db')
    invoice_total = pd.read_sql_query(
            """
                SELECT InvoiceId, InvoiceDate, (customers.FirstName||' '||customers.LastName) as CustomerName, Country, City, Total
                FROM invoices
                LEFT JOIN customers ON customers.CustomerID = invoices.CustomerID
            """, conn)
    invoice_total['InvoiceDate'] = pd.to_datetime(invoice_total['InvoiceDate'])
    year_order = ["2009", "2010", "2011", "2012", "2013"]
    invoice_total['Year'] = invoice_total['InvoiceDate'].dt.year

    invoice_total['Year'] = pd.Categorical(invoice_total['Year'], categories=year_order)
    invoice_total['Year'] = invoice_total['InvoiceDate'].dt.year
    invoice_total = invoice_total.groupby(by=["InvoiceDate", "CustomerName", "Country", "Year"])["Total"].agg("sum").reset_index()
    
    mask = invoice_total['Year'] == year
    data = invoice_total[mask].to_json()

    res = Response(response = data, status = 200, mimetype = "application/json")

    return (res)

@app.route('/docs')
def get_docs():
    return template.render()

@app.route('/')
def index():
    return template.render()

if __name__ == '__main__':
    app.run(debug=True, port=5000)