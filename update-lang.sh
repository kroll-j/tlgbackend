#!/bin/bash
# this script:
# - updates the translation template file $MSGDIR/$DOMAIN.pot
# - updates .po files with new translatable strings in source code
# - compiles .po files to .mo files

# directory where localization files are placed
MSGDIR=messages

# textdomain
DOMAIN=tlgbackend

# find languages
LANGUAGES=$(cd $MSGDIR; find -mindepth 1 -maxdepth 1 -type d -execdir basename '{}' ';')

SRC=$(ls *.py filtermodules/*.py)

TMPPOT=$(mktemp)
#xgettext -d $DOMAIN $SRC --keyword=_ -o - | sed "s/CHARSET/UTF-8/" > $TMPPOT &&
pygettext -o - $SRC | sed "s/CHARSET/UTF-8/" > $TMPPOT &&
echo -n "merging new strings into template file $MSGDIR/$DOMAIN.pot " &&
msgmerge -U $MSGDIR/$DOMAIN.pot $TMPPOT &&
rm $TMPPOT &&

for LANG in $LANGUAGES; do
	echo -n "merging new strings into $LANG " &&
	msgmerge -U $MSGDIR/$LANG/LC_MESSAGES/$DOMAIN.po $MSGDIR/$DOMAIN.pot &&
	echo generating binary message catalog for $LANG &&
	msgfmt -c -v -o $MSGDIR/$LANG/LC_MESSAGES/$DOMAIN.mo $MSGDIR/$LANG/LC_MESSAGES/$DOMAIN.po
done

