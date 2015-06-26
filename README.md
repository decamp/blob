### Blob

BLOB is a single class.  
BLOB is a flexible container that operates on nested Maps, Lists, Sets, and primitives.  
BLOB is an API for manipulating YAML/JSON data.  

Blob is single class: a container for manipulating JSON or YAML data.
YAML and JSON are great, but produce nested object collections that can be 
difficult to work with in Java because every of all the type checking and
casting required. Blob simplifies the process and makes it possible to
working with nested data structures with minimal coding.


### Build:
$ ant


### Runtime:
After build, add all jars in **lib** and **target** directories to your project.


### Dependencies:
SnakeYAML. 

It's easy enough to swap out other YAML/JSON libraries though. I've used Blob
with Jackson, JSONObject, and a couple of others.


### Licenses:
Blob is licensed under a BSD license, included as LICENSE.TXT.

SnakeYAML is a third-party library.
It's license is included at "lib/snakeyaml/SNAKEYAML_LICENSE".


---
Author: Philip DeCamp
