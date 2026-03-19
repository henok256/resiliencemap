-- ResilienceMap PostGIS Schema Initialization
-- Run automatically by Docker on first startup

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Census tracts
CREATE TABLE IF NOT EXISTS census_tracts (
    id          SERIAL PRIMARY KEY,
    geoid       CHAR(11) NOT NULL UNIQUE,
    state_fips  CHAR(2)  NOT NULL,
    county_fips CHAR(5)  NOT NULL,
    name        VARCHAR(100),
    land_area_sqm FLOAT,
    geom        GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_census_tracts_geom      ON census_tracts USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_census_tracts_county    ON census_tracts(county_fips);
CREATE INDEX IF NOT EXISTS idx_census_tracts_state     ON census_tracts(state_fips);

-- FEMA flood zones
CREATE TABLE IF NOT EXISTS flood_zones (
    id          SERIAL PRIMARY KEY,
    fld_zone    VARCHAR(17) NOT NULL,
    zone_subty  VARCHAR(72),
    sfha_tf     CHAR(1),     -- T = Special Flood Hazard Area, F = not
    state_fips  CHAR(2),
    geom        GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_flood_zones_geom       ON flood_zones USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_flood_zones_state      ON flood_zones(state_fips);
CREATE INDEX IF NOT EXISTS idx_flood_zones_zone       ON flood_zones(fld_zone);

-- USGS seismic events
CREATE TABLE IF NOT EXISTS seismic_hazard (
    id          SERIAL PRIMARY KEY,
    usgs_id     VARCHAR(20) NOT NULL UNIQUE,
    magnitude   FLOAT NOT NULL,
    depth_km    FLOAT,
    place       VARCHAR(255),
    event_time  TIMESTAMP NOT NULL,
    geom        GEOMETRY(POINT, 4326) NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_seismic_geom      ON seismic_hazard USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_seismic_time      ON seismic_hazard(event_time);

-- NOAA storm alerts
CREATE TABLE IF NOT EXISTS storm_alerts (
    id          SERIAL PRIMARY KEY,
    noaa_id     VARCHAR(100) NOT NULL UNIQUE,
    event       VARCHAR(100) NOT NULL,
    severity    VARCHAR(50),
    certainty   VARCHAR(50),
    headline    TEXT,
    description TEXT,
    effective   TIMESTAMP,
    expires     TIMESTAMP,
    geom        GEOMETRY(MULTIPOLYGON, 4326),
    ingested_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_storm_alerts_geom    ON storm_alerts USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_storm_alerts_expires ON storm_alerts(expires);

-- Wildfire incidents (NIFC active fire perimeters)
CREATE TABLE IF NOT EXISTS wildfire_incidents (
    id                SERIAL PRIMARY KEY,
    irwin_id          VARCHAR(64) NOT NULL UNIQUE,
    incident_name     VARCHAR(255) NOT NULL,
    acres_burned      FLOAT,
    percent_contained FLOAT,
    state_fips        CHAR(2),
    fire_cause        VARCHAR(100),
    start_date        TIMESTAMP,
    updated_at        TIMESTAMP,
    geom              GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    ingested_at       TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wildfire_geom  ON wildfire_incidents USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_wildfire_state ON wildfire_incidents(state_fips);
CREATE INDEX IF NOT EXISTS idx_wildfire_start ON wildfire_incidents(start_date);

-- Critical infrastructure facilities (HIFLD)
CREATE TABLE IF NOT EXISTS critical_infrastructure (
    id              SERIAL PRIMARY KEY,
    hifld_id        VARCHAR(64) NOT NULL UNIQUE,
    facility_type   VARCHAR(20) NOT NULL,  -- 'hospital', 'school', 'power_plant'
    name            VARCHAR(255) NOT NULL,
    address         VARCHAR(500),
    city            VARCHAR(100),
    state_fips      CHAR(2),
    county_fips     CHAR(5),
    capacity        INTEGER,               -- beds / enrollment / MW
    status          VARCHAR(50),
    latitude        FLOAT NOT NULL,
    longitude       FLOAT NOT NULL,
    geom            GEOMETRY(POINT, 4326) NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_infra_geom     ON critical_infrastructure USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_infra_state    ON critical_infrastructure(state_fips);
CREATE INDEX IF NOT EXISTS idx_infra_type     ON critical_infrastructure(facility_type);

-- FEMA disaster declarations (historical, 20+ years)
CREATE TABLE IF NOT EXISTS disaster_declarations (
    id                  SERIAL PRIMARY KEY,
    disaster_number     INTEGER NOT NULL,
    fema_id             VARCHAR(30) NOT NULL UNIQUE,
    state               CHAR(2) NOT NULL,
    state_fips          CHAR(2),
    county_fips         CHAR(5),
    declaration_type    VARCHAR(2) NOT NULL,
    incident_type       VARCHAR(50) NOT NULL,
    declaration_title   VARCHAR(255),
    declaration_date    TIMESTAMP NOT NULL,
    incident_begin_date TIMESTAMP,
    incident_end_date   TIMESTAMP,
    designated_area     VARCHAR(255),
    fema_region         INTEGER,
    ingested_at         TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_disaster_state  ON disaster_declarations(state);
CREATE INDEX IF NOT EXISTS idx_disaster_fips   ON disaster_declarations(county_fips);
CREATE INDEX IF NOT EXISTS idx_disaster_date   ON disaster_declarations(declaration_date);
CREATE INDEX IF NOT EXISTS idx_disaster_type   ON disaster_declarations(incident_type);
CREATE INDEX IF NOT EXISTS idx_disaster_year   ON disaster_declarations(EXTRACT(YEAR FROM declaration_date));

-- FEMA disaster cost summaries
CREATE TABLE IF NOT EXISTS disaster_costs (
    id                     SERIAL PRIMARY KEY,
    disaster_number        INTEGER NOT NULL UNIQUE,
    total_ihp_approved     FLOAT DEFAULT 0.0,
    total_ha_approved      FLOAT DEFAULT 0.0,
    total_ona_approved     FLOAT DEFAULT 0.0,
    total_pa_obligated     FLOAT DEFAULT 0.0,
    total_hmgp_obligated   FLOAT DEFAULT 0.0,
    total_cost             FLOAT DEFAULT 0.0,
    ingested_at            TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cost_disaster ON disaster_costs(disaster_number);
CREATE INDEX IF NOT EXISTS idx_cost_total    ON disaster_costs(total_cost DESC);

-- Risk scores (one row per tract, updated on each scoring run)
CREATE TABLE IF NOT EXISTS risk_scores (
    id                        SERIAL PRIMARY KEY,
    tract_geoid               CHAR(11) NOT NULL,
    county_fips               CHAR(5)  NOT NULL,
    flood_score               FLOAT DEFAULT 0.0,
    seismic_score             FLOAT DEFAULT 0.0,
    storm_score               FLOAT DEFAULT 0.0,
    wildfire_score            FLOAT DEFAULT 0.0,
    social_vulnerability_score FLOAT DEFAULT 0.0,
    composite_score           FLOAT NOT NULL,
    computed_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE(tract_geoid)
);
CREATE INDEX IF NOT EXISTS idx_risk_scores_county    ON risk_scores(county_fips);
CREATE INDEX IF NOT EXISTS idx_risk_scores_composite ON risk_scores(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_risk_scores_computed  ON risk_scores(computed_at);
