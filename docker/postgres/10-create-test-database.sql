-- Separate database for the integration tests, so `make test` never touches live data.
CREATE DATABASE control_plane_test;
