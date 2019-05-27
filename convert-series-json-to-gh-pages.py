from json2html import *
import json 

series_template = """
<html lang="en">

<head>

  <meta name="description"
    content="Sustainable Development Goals: Series">
  <meta name="author" content="United Nations Statistics Division Statistical Services Branch">
<meta name="author" content="EPISTEMIK Szymon Klarman">

  <title>Sustainable Development Goals: Series</title>
  <h1>Sustainable Development Goals: Series</h1>


  <br>

  <h2>{}</h2>


  <h2>Series identifier: {}</h2>
  <br>

  <link href="../tables.css" rel="stylesheet">

  <script type="application/ld+json">
  {}
  </script>

</head>

<body>

 {}

</body>

</html>
"""

index_template = """
<html lang="en">

<head>

  <meta name="description"
    content="Sustainable Development Goals: Series">
  <meta name="author" content="United Nations Statistics Division Statistical Services Branch">
<meta name="author" content="EPISTEMIK Szymon Klarman">

  <title>Sustainable Development Goals: Series</title>
  <h1>Sustainable Development Goals: Series</h1>


  <br>



  <link href="tables.css" rel="stylesheet">

</head>

<body>

 <table border="1">
 
    
    {}
    

 </table>

</body>

</html>
"""

item_template = """<tr> <td> <a href={}> {} </a> </td> </tr> \n"""


with open('sdg-data/dataset-metadata-descriptions.json') as f:
    corpus = json.load(f)

index_file = open("index.html", "w")

series_index = {}

for series in corpus["@graph"]:
    series["@context"] = "http://schema.org/"
    output = json2html.convert(json = series)
    html = series_template.format(series["name"], series["identifier"], json.dumps(series), output)
    name = series["identifier"] + ".html"
    url = "series_html/" + name
    series_index[series["description"]] = url
    with open(url, "w") as file:
        file.write(html)
    file.close()



keys = series_index.keys()
key_list = list(keys)
key_list.sort()
items = ""

for key in key_list:
    items += item_template.format(series_index[key], key)

index_html = index_template.format(items)
index_file.write(index_html)
index_file.close()


