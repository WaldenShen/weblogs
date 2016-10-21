CREATE TABLE adv_pagecorr (
  url_start text,
  url_end text,
  url_type varchar(32),
  date_type varchar(8),
  creation_datetime varchar(32),
  n_count int,
  percentage real,
  chain_length int
);

CREATE INDEX index_url_type ON adv_pagecorr(url_type);
CREATE INDEX index_creation_datetime ON adv_pagecorr(date_type,creation_datetime);

CREATE TABLE adv_retention (
  login_datetime text,
  creation_datetime text,
  return_1 int,
  return_2 int,
  return_3 int,
  return_4 int,
  return_5 int,
  return_6 int,
  return_7 int,
  return_14 int,
  return_21 int,
  return_28 int,
  return_56 int,
  no_return int
);

CREATE TABLE history_cookie (
  individual_id varchar(128),
  cookie_id varchar(128),
  category_type varchar(32),
  times int,
  creation_datetime timestamp,
  prev_interval real,
  category_key varchar(128),
  category_value int,
  total_count int
);

CREATE INDEX index_total_count ON history_cookie(total_count);
CREATE INDEX index_creation_datetime_history_cookie ON history_cookie(creation_datetime);
CREATE INDEX index_category_type ON history_cookie(category_type);

CREATE TABLE mapping_id (
  cookie_id text,
  individual_id text,
  creation_datetime text
);

CREATE TABLE stats_cookie (
  category_key varchar(32),
  category_value varchar(128),
  date_type varchar(8),
  creation_datetime varchar(32),
  n_count real
);

CREATE INDEX index_creation_datetime_stats_cookie ON stats_cookie(date_type,creation_datetime);
CREATE INDEX index_category_key ON stats_cookie(category_key);

CREATE TABLE stats_nal (
  creation_datetime text,
  count_new_pv int,
  count_new_uv int,
  count_old_pv int,
  count_old_uv int
);

CREATE TABLE stats_page (
  url text,
  url_type varchar(32),
  date_type varchar(8),
  creation_datetime varchar(32),
  page_view int,
  user_view int,
  profile_view int,
  duration real,
  active_duration real,
  loading_duration real,
  PRIMARY_KEY (date_type, creation_datetime)
);

CREATE TABLE stats_page_y2016m06 (
  CHECK (creation_datetime >= '2016-06-01' AND creation_datetime < '2016-07-01')
) INHERITS (stats_page);

CREATE TABLE stats_page_y2016m07 (
  CHECK (creation_datetime >= '2016-07-01' AND creation_datetime < '2016-08-01')
) INHERITS (stats_page);

CREATE TABLE stats_page_y2016m08 (
  CHECK (creation_datetime >= '2016-08-01' AND creation_datetime < '2016-09-01')
) INHERITS (stats_page);

CREATE TABLE stats_page_y2016m09 (
  CHECK (creation_datetime >= '2016-09-01' AND creation_datetime < '2016-10-01')
) INHERITS (stats_page);

CREATE TABLE stats_page_y2016m10 (
  CHECK (creation_datetime >= '2016-10-01' AND creation_datetime < '2016-11-01')
) INHERITS (stats_page);

CREATE TABLE stats_page_y2016m11 (
  CHECK (creation_datetime >= '2016-11-01' AND creation_datetime < '2016-12-01')
) INHERITS (stats_page);

CREATE TABLE stats_page_y2016m12 (
  CHECK (creation_datetime >= '2016-12-01' AND creation_datetime < '2017-01-01')
) INHERITS (stats_page);

CREATE INDEX index_creation_datetime_stats_page ON stats_page(creation_datetime,date_type);
CREATE INDEX index_url_type_stats_page ON stats_page(url_type);

CREATE TABLE stats_session (
  category_key varchar(32),
  category_value varchar(256),
  date_type varchar(16),
  creation_datetime varchar(32),
  n_count real
);

CREATE INDEX index_creation_datetime_stats_session ON stats_session(date_type,creation_datetime);
CREATE INDEX index_category_key_stats_session ON stats_session(category_key);

CREATE TABLE stats_website (
  domain varchar(128),
  date_type varchar(16),
  creation_datetime varchar(32),
  page_view int,
  user_view int,
  profile_view int,
  duration real,
  active_duration real,
  loading_duration real,
  count_failed int,
  count_session int
);

CREATE INDEX index_domain ON stats_website(domain);
CREATE INDEX index_creation_datetime_stats_website ON stats_website(date_type,creation_datetime);
