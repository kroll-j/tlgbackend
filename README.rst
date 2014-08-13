Article List Generator for Wikipedia - Backend
===========================================
\(c) Wikimedia Deutschland

Author: Johannes Kroll

The Article List Generator makes it possible to search categories and compile article lists using different criteria. The query may consist of a one or more categories, the intersection or the difference of categories. The user is able to determine the depth of the search and a set of filters shown on the righthand side allows further refinement of the result. The combination of these filters is also possible.

The ALG backend uses graphserv and the SQL database to search and filter pages. It can be used with the `front end <http://tools.wmflabs.org/render/stools/alg>`_ or `stand-alone <http://tools.wmflabs.org/render/tlgbe/tlgwsgi.py>`_.
