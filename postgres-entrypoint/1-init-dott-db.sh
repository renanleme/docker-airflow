#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "airflow" "" <<-EOSQL
    CREATE DATABASE dott;
    \c dott;
    CREATE TABLE public.rides (
		ride_id varchar(20) NOT NULL,
		vehicle_id varchar(20) NOT NULL,
		time_ride_start timestamp NULL,
		time_ride_end timestamp NULL,
		start_lat float8 NULL,
		start_lng float8 NULL,
		end_lat float8 NULL,
		end_lng float8 NULL,
		gross_amount decimal(10,2) NULL,
		ride_distance float8 NULL
	);

 	CREATE INDEX 
	 	idx_rides_vehicle_id
	ON
	 rides(vehicle_id);


	CREATE TABLE public.deployments (
		task_id varchar(20) NOT NULL,
		vehicle_id varchar(20) NOT NULL,
		time_task_created timestamp NULL,
		time_task_resolved timestamp NULL
	);

	CREATE TABLE public.pickups (
		task_id varchar(20) NOT NULL,
		vehicle_id varchar(20) NOT NULL,
		qr_code varchar(6) NULL,
		time_task_created timestamp NULL,
		time_task_resolved timestamp NULL
	);

	CREATE TABLE public.last_cycle (
		rides_quantity int8 NOT NULL,
		vehicle_id varchar(20) NOT NULL,
		time_last_deploy timestamp NULL,
		time_last_pickup timestamp NULL
	);

	CREATE INDEX 
		idx_last_cycle_vehicle_id
	ON 
		last_cycle(vehicle_id);

	CREATE TABLE public.log_duplicated_rides (
		ride_id varchar(20) NOT NULL,
		vehicle_id varchar(20) NOT NULL,
		time_ride_start timestamp NULL,
		time_ride_end timestamp NULL,
		start_lat float8 NULL,
		start_lng float8 NULL,
		end_lat float8 NULL,
		end_lng float8 NULL,
		gross_amount decimal(10,2) NULL,
		ride_distance float8 NULL,
		landing_date timestamp NOT NULL
	);

	CREATE TABLE public.log_duplicated_deployments (
		task_id varchar(20) NULL,
		vehicle_id varchar(20) NOT NULL,
		time_task_created timestamp NULL,
		time_task_resolved timestamp NULL,
		landing_date timestamp NOT NULL
	);

	CREATE TABLE public.log_duplicated_pickups (
		task_id varchar(20) NOT NULL,
		vehicle_id varchar(20) NOT NULL,
		qr_code varchar(6) NULL,
		time_task_created timestamp NULL,
		time_task_resolved timestamp NULL,
		landing_date timestamp NOT NULL
	);
EOSQL