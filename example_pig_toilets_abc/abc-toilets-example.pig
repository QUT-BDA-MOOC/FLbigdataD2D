--
-- Big Data Workshop 2012.10.03. Example by Tomasz Bednarz / CSIRO 
-- 
-- Find closest toilets to ABC radio stations. 
--
-- ABC local stations
-- Source http://www.abc.net.au/local/data/public/stations/abc-local-radio.csv
-- (a) state,
-- (b) website-URL,
-- (c) station, 
-- (d) town 
-- (e) latitude,
-- (f) longitude,
-- (g) talkback-number,
-- (h) Enquiries-number,
-- (i) Fax-number
-- (j) Sms-number
-- (k) Street-number
-- (l) Street-suburb
-- (m) Street-postcode
-- (n) PO-box
-- (o) PO-suburb
-- (p) PO-postcode
-- (r) Twitter
-- (s) Facebook

-- National Public Toilet Map
-- Source http://data.gov.au/dataset/national-public-toilet-map/
-- (a) toilet name;
-- (b) address;
-- (c) latitude and longitude;
-- (d) general toilet features;
-- (e) location;
-- (f) accessibility;
-- (g) opening hours;
-- (h) additional features (e.g. showers, baby change facilities etc);
-- (i) notes (e.g. coin operated showers etc).

REGISTER piggybank.jar;
DEFINE RegexE org.apache.pig.piggybank.evaluation.string.RegexExtractAll();

REGISTER datafu-0.0.6-SNAPSHOT.jar;
DEFINE HaversineMiles datafu.pig.geo.HaversineDistInMiles();

all_abc_st = load 'abc-local-radio.csv' using PigStorage(',');
-- dump all_abc_st;

all_toilets = load 'ToiletmapExport-Small.xml' using org.apache.pig.piggybank.storage.XMLLoader('ToiletDetails') as (toilet:chararray);
-- all_toilets = load 'ToiletmapExport.xml' using org.apache.pig.piggybank.storage.XMLLoader('ToiletDetails') as (toilet:chararray);
-- dump all_toilets;

rev = FOREACH all_toilets GENERATE FLATTEN(
--	RegexE('tomek test', '^(\\w+)\\s(\\w+)$')
	RegexE(toilet, 
		'^<ToiletDetails\\sxmlns=\\"http:\\/\\/[\\dA-Za-z\\.\\-\\/]+\\"\\sxmlns:xsi=\\"http:\\/\\/[\\dA-Za-z\\.\\-\\/]+\\"\\sxsi:schemaLocation=\\"http:\\/\\/[\\d\\sA-Za-z\\_\\:\\.\\-\\/]+\\"\\sStatus=\\"[a-zA-Z]+\\"\\sLatitude=\\"([\\-\\+\\d.]+)\\"\\sLongitude=\\"([\\-\\+\\d.]+)\\"\\sToiletURL=[\\"\\dA-Za-z:\\/\\s\\.\\?\\-\\=\\&\\>\\u003B]+\\<Name\\>([A-za-z\\-\\d\\s\\u0027\\,\\&\\u003B]+)\\<\\/Name\\>[A-Za-z\\d\\,\\(\\)\\<\\>\\/\\-\\s\\&\\u003B]+\\<State\\>([A-Za-z\\s\\.]+)\\<\\/State\\>[\\"\\sA-Za-z\\d\\<\\>\\=\\/\\-\\.\\:\\,\\(\\)\\u003B\\u0027]+\\<\\/ToiletDetails\\>$')
	)
	as 
	(
		wclati: chararray,
		wclong: chararray,
		wcname: chararray,
		wcstate: chararray
	);
-- dump rev;

projected_abc = foreach all_abc_st generate (double) $4 as abc_lat, (double) $5 as abc_long, $2 as abc_name;
-- dump projected_abc;

--notice: converting chararray to double
projected_wc = foreach rev generate (double) wclati as wc_lat, (double) wclong as wc_long, wcname as wc_name;
-- dump projected_wc;

-- produce all pairs of abc-stations vs toilets and calculate distance between localisations

pairs_abc_wc = cross projected_abc, projected_wc;
-- dump pairs_abc_wc;

distances_abc_wc = foreach pairs_abc_wc generate abc_name, wc_name, HaversineMiles(abc_lat, abc_long, wc_lat, wc_long) as distance, abc_lat, abc_long, wc_lat, wc_long;
-- dump distances_abc_wc;

-- for each abc_station, get the closest one
grouped = group distances_abc_wc by abc_name;
-- dump grouped;
closest = foreach grouped {
	ordered = order distances_abc_wc by distance ASC;
	clo = limit ordered 1;
	generate flatten(clo);
} 
dump closest;

to_display = foreach closest generate 
CONCAT('[', (chararray)abc_lat), abc_long, 
CONCAT('"', CONCAT(abc_name, '"')), 
wc_lat, wc_long, 
CONCAT('"', CONCAT(wc_name, '"')), 
CONCAT((chararray)distance, '],');

store to_display into 'map.txt' using PigStorage(',');
