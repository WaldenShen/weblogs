use clickstream;

CREATE TABLE `adv_pagecorr` (
  `url_start` text,
  `url_end` text,
  `url_type` varchar(32) DEFAULT NULL,
  `date_type` varchar(8) DEFAULT NULL,
  `creation_datetime` varchar(32) DEFAULT NULL,
  `n_count` int(11) DEFAULT NULL,
  `percentage` double DEFAULT NULL,
  `chain_length` int(11) DEFAULT NULL,
  KEY `index_url_type` (`url_type`),
  KEY `index_creation_datetime` (`date_type`,`creation_datetime`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `adv_retention` (
  `login_datetime` text,
  `creation_datetime` text,
  `return_1` int(11) DEFAULT NULL,
  `return_2` int(11) DEFAULT NULL,
  `return_3` int(11) DEFAULT NULL,
  `return_4` int(11) DEFAULT NULL,
  `return_5` int(11) DEFAULT NULL,
  `return_6` int(11) DEFAULT NULL,
  `return_7` int(11) DEFAULT NULL,
  `return_14` int(11) DEFAULT NULL,
  `return_21` int(11) DEFAULT NULL,
  `return_28` int(11) DEFAULT NULL,
  `return_56` int(11) DEFAULT NULL,
  `no_return` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `history_cookie` (
  `individual_id` varchar(128) DEFAULT NULL,
  `cookie_id` varchar(128) DEFAULT NULL,
  `category_type` varchar(32) DEFAULT NULL,
  `times` int(11) DEFAULT NULL,
  `creation_datetime` datetime DEFAULT NULL,
  `prev_interval` double DEFAULT NULL,
  `category_key` varchar(128) DEFAULT NULL,
  `category_value` int(11) DEFAULT NULL,
  `total_count` int(11) DEFAULT NULL,
  KEY `index_total_count` (`total_count`) USING BTREE,
  KEY `index_creation_datetime` (`creation_datetime`) USING BTREE,
  KEY `index_category_type` (`category_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY KEY (category_type)
PARTITIONS 8 */;

CREATE TABLE `mapping_id` (
  `cookie_id` text,
  `individual_id` text,
  `creation_datetime` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stats_cookie` (
  `category_key` varchar(32) DEFAULT NULL,
  `category_value` varchar(128) DEFAULT NULL,
  `date_type` varchar(8) DEFAULT NULL,
  `creation_datetime` varchar(32) DEFAULT NULL,
  `n_count` double DEFAULT NULL,
  KEY `index_creation_datetime` (`date_type`,`creation_datetime`) USING BTREE,
  KEY `index_category_key` (`category_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stats_nal` (
  `creation_datetime` text,
  `count_new_pv` int(11) DEFAULT NULL,
  `count_new_uv` int(11) DEFAULT NULL,
  `count_old_pv` int(11) DEFAULT NULL,
  `count_old_uv` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stats_page` (
  `url` text,
  `url_type` varchar(32) DEFAULT NULL,
  `date_type` varchar(8) DEFAULT NULL,
  `creation_datetime` varchar(32) DEFAULT NULL,
  `page_view` int(11) DEFAULT NULL,
  `user_view` int(11) DEFAULT NULL,
  `profile_view` int(11) DEFAULT NULL,
  `duration` double DEFAULT NULL,
  `active_duration` double DEFAULT NULL,
  `loading_duration` double DEFAULT NULL,
  KEY `index_creation_datetime` (`creation_datetime`,`date_type`) USING BTREE,
  KEY `index_url_type` (`url_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stats_session` (
  `category_key` varchar(32) DEFAULT NULL,
  `category_value` varchar(256) DEFAULT NULL,
  `date_type` varchar(16) DEFAULT NULL,
  `creation_datetime` varchar(32) DEFAULT NULL,
  `n_count` double DEFAULT NULL,
  KEY `index_creation_datetime` (`date_type`,`creation_datetime`) USING BTREE,
  KEY `index_category_key` (`category_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stats_website` (
  `domain` varchar(128) DEFAULT NULL,
  `date_type` varchar(16) DEFAULT NULL,
  `creation_datetime` varchar(32) DEFAULT NULL,
  `page_view` int(11) DEFAULT NULL,
  `user_view` int(11) DEFAULT NULL,
  `profile_view` int(11) DEFAULT NULL,
  `duration` double DEFAULT NULL,
  `active_duration` double DEFAULT NULL,
  `loading_duration` double DEFAULT NULL,
  `count_failed` int(11) DEFAULT NULL,
  `count_session` int(11) DEFAULT NULL,
  KEY `index_domain` (`domain`),
  KEY `index_creation_datetime` (`date_type`,`creation_datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
