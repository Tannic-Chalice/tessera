# Bengaluru road network (used by the live feed)

`tessera/data/bengaluru_roads.json.gz` holds the road geometry the simulated vehicles drive on:
3,847 BMTC bus-route segments (~49,600 km of route geometry, many arterials are shared by
several routes) inside a 14 km radius of central Bengaluru, plus 22 known traffic bottlenecks
(Central Silk Board, Hebbal Flyover, KR Puram / Tin Factory, Marathahalli Bridge, ...).

Source: unofficial BMTC GTFS data set by Vonter — https://github.com/Vonter/bmtc-gtfs —
scraped from the official Namma BMTC app. Licence: Open Database License (ODbL) 1.0
(see LICENSE-ODbL.txt). Processing: routes clipped to the 14 km circle, split at gaps > 600 m,
simplified (Ramer-Douglas-Peucker, 8 m tolerance), segments shorter than 1.5 km dropped.
Landmark coordinates are the BMTC stop locations of the same data set.
