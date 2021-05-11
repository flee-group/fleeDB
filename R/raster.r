#' Get raster datasets
#' 
#' @param conn A database connection, e.g., from [flee_db_connect()]
#' @param ras The name of the raster dataset to retrieve; 
#' 		if missing, a list of datasets is returned
fleedb_raster = function(conn, ras, file) {

	meta = data.table::data.table(DBI::dbReadTable(conn, "rasters.raster_meta"))
	if(missing(ras)) {
		return(meta$table_name)
	}

	r = DBI::dbReadTable(conn, ras)
	r = raster::rasterFromXYZ(r, crs = crs)
}