#' Connect to the fleeDB
#' @param key_name The name of the fleeDB key stored on your computer
#' @return A db connection from the RPostgres package
#' @export
fdb_connect = function(key_name = "fleedb-login-c7701212", user = "c7701212") {
	key = keyring::key_get(key_name)
	host = "db06.intra.uibk.ac.at"
	dbname = "c7701212"
	RPostgres::dbConnect(RPostgres::Postgres(), host = host, user = user, password = key, 
		dbname = dbname)
}
