# GND ranker
This is a Wikidata bot that manages ranking of GND identifiers with these issues:
* GND identifiers for entries with "Type n" (Tn). These are "undifferentiated" entries that are technically no longer considered to be part of GND. However, there are plenty of them and they are still accessible in the GND web user interface, thus editors mistakenly import them to Wikidata as well. These entries should be lowered to "deprecated rank".
* GND identifiers that redirect to another entry. These are presumably former duplicates within the GND database. While still somewhat valid, in particular for identification, users usually want to use the latest, non-redirecting identifier instead. The redirecting identifier is thus lowered to "deprecated rank", and the redirect target is also being added in case it is still missing from the data item.
* Invalid GND identifiers that formally look like correct ones, but are not. These are set to "deprecated rank" and equipped with a suitable qualifier in order to allow the community to manually review these cases. In many cases, a removal is probably okay.
* The bot also simplfies odd ranking situations in data items with GND claims that have "preferred rank", but no GND claims with "normal rank". Since "preferred rank" does not add any value, but could confuse editors, the claim(s) with "preferred rank" are lowered to the standard "normal rank".

There are also auxiliary scripts called `helper_*.py` in the main bot directory that are meant to be run manually to evaluate special cases without making edits to the wiki. They produce `*.tsv` files in `output/` instead.

## Technical requirements
The bot is currently scheduled to run weekly on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) from within the `msynbot` tool account. It depends on the [shared pywikibot files](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Pywikibot#Using_the_shared_Pywikibot_files_(recommended_setup)) and is running in a Kubernetes environment using Python 3.9.2.