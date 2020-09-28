PLUGIN_VERSION=0.2.8
PLUGIN_ID=events-aggregator

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env custom-recipes plugin.json python-lib
