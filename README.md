# Superflex

You will find this library almost unusable, although I find I still use it all the time.

It is a library for importing collections of textfiles into a (postgres) database table, "designed" to be extensible
for almost any conceivable format of large datasets often split (horizontally or vertically) into multiple files.

"Designed" goes in quotes. This was my very first significant Scala project in 2009 (built Scala 2.7 origially).
It's a bit horrible, a complicated customize-by-inheritance-based API, full of ad-hoc features I added as some
ever more exotic dataset had to be added to the little data warehouse I built. It did its job well, but it's
almost entirely undocumented and I am forever trying to remember how things worked and what it can and can't do.

I should rewrite it sanely. Or at the very least, document it. Someday.

If by some chance you are not me and you find the courage and the means to use this library, let me know.



