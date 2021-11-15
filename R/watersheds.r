#' Create a watershed in the database
#'
#' @param conn A [fleeDB_connection()] with write permissions
#' @param name The watershed name (e.g., "vjosa")
#' @param schema Where to write it, normally do not change the default
#' @return NULL, has the side effect of creating watershed tables in the database
#' @export
create_watershed = function(conn, name, schema = "watersheds") {

	queries = list(
		q0 = "begin;",
		q1_stream = paste0(
			"create table ", schema, ".", name, "_stream (",
				"reach_id serial primary key, ",
				"catchment_area_km2 real, ",
				"geometry geometry);"),
		q2_topology_r = paste0(
			"create table ", schema, ".", name, "_reach_topology (",
				"tpr_id serial primary key,",
				"from_id integer not null references ", schema, ".", name, "_stream(reach_id), ",
				"to_id integer not null references ", schema, ".", name, "_stream(reach_id), ",
				"reach_len_m real);"),
		q3_pixels = paste0(
			"create table ", schema, ".", name, "_pixels (",
				"pix_id serial primary key, ",
				"reach_id integer not null references ", schema, ".", name, "_stream(reach_id), ",
				"drainage integer, ",
				"catchment_area_km2 real, ",
				"elevation_m real, ",
				"geometry geometry);"),
		q4_pixtime = paste0(
			"create table ", schema, ".", name, "_pixel_time (",
				"pixtime_id serial primary key, ",
				"pix_id integer not null references ", schema, ".", name, "_pixels(pix_id), ",
				"expedition_id integer not null references flee.expeditions(expedition_id), ",
				"discharge_m3_per_s real, ",
				"velocity_m_per_s real, ",
				"depth_m real, ",
				"width_m real, ",
				"constraint ", name, "_u_pix_expedition unique (pix_id, expedition_id));"),
		q5_topology_p = paste0(
			"create table ", schema, ".", name, "_pixel_topology (",
				"tp_id serial primary key, ",
				"from_id integer not null references ", schema, ".", name, "_pixels(pix_id), ",
				"to_id integer not null references ", schema, ".", name, "_pixels(pix_id), ",
				"pix_len_m real);"),
		q6 = "commit;")

	val = lapply(queries, function(q) RPostgres::dbExecute(conn, q))
	return(NULL)
}

#' Writes watershed data to the database
#'
#' @details Column names in the tables MUST match those in the database.
#' @param conn A [fleeDB_connection()] with write permissions
#' @param name The watershed name (e.g., "vjosa")
#' @param stream A vector map of streams, in sf format
#' @param pixels A data.frame of pixel data, with IDs
#' @param pix_time A data.frame of pixel-time data (e.g., discharge etc)
#' @param Tp A pixel topology, in SparseMatrix format
#' @param Tr A reach topology SparseMatrix
#' @param schema Where to write it, normally do not change the default
#' @return NULL, has the side effect of writing watershed tables in the database
#' @export
write_watershed = function(conn, name, stream, pixels, pix_time, Tp, Tr, schema = "watersheds") {
	# streams first
	if(!missing(stream))
		sf::st_write(stream[, c("reach_id", "catchment_area_km2", "geometry")], dsn = conn, 
			layer = RPostgres::Id(schema = schema, table = paste0(name, "_stream")), append = TRUE)

	# reach topology
	if(!missing(Tr)) {
		Tr = as(Tr, "TsparseMatrix")
		Trdt = data.frame(from_id = Tr@i+1, to_id = Tr@j+1, reach_len_m = Tr@x)
		RPostgres::dbWriteTable(conn, 
			RPostgres::Id(schema = schema, table = paste0(name, "_reach_topology")), Trdt, 
			append = TRUE)

	}

	# pixels
	if(!missing(pixels)) {
		sf::st_write(pixels[, c("pix_id", "reach_id", "drainage", "catchment_area_km2", "elevation_m", 
			"geometry")], dsn = conn, 
			layer = RPostgres::Id(schema = schema, table = paste0(name, "_pixels")), append = TRUE)
	}

	# pixel-time data
	if(!missing(pix_time)) {
	RPostgres::dbWriteTable(conn, 
		RPostgres::Id(schema = schema, table = paste0(name, "_pixel_time")), 
		pix_time[, c("pix_id", "expedition_id", "discharge_m3_per_s", "velocity_m_per_s", 
			"depth_m", "width_m")], append = TRUE)
	}

	# pixel topology
	if(!missing(Tp)) {
		Tp = as(Tp, "TsparseMatrix")
		Tpdt = data.frame(from_id = Tp@i+1, to_id = Tp@j+1, pix_len_m = Tp@x)
		RPostgres::dbWriteTable(conn, 
			RPostgres::Id(schema = schema, table = paste0(name, "_pixel_topology")), Tpdt, 
			append = TRUE)
	}

	return(NULL)
}

#' Read stream map from the database
#' @param conn A [fleeDB_connection()] with write permissions
#' @param name The watershed name (e.g., "vjosa")
#' @param schema Where to write it, normally do not change the default
#' @return A map of streams in sf format
#' @export
fdb_streams = function(conn, name, schema = "watersheds") {
	sf::st_read(conn, layer = RPostgres::Id(schema = schema, table = paste0(name, "_stream")))
}

