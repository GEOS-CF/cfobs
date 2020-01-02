Collection of routines to compare NASA GEOS-CF model output against (point-source) observations. 

The core element is the CFObs class that contains a Pandas DataFrame with point source observation data and the corresponding GEOS-CF model value. The data frame is created by first reading the point source observations and then adding the corresponding CF output to it. See examples for more details.
