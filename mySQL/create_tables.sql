CREATE TABLE clean_data (
master_uuid VARCHAR(255) PRIMARY KEY,
author VARCHAR(255),
entities_locations VARCHAR(255),
entities_organizations VARCHAR(255),
entities_persons VARCHAR(255),
url VARCHAR(255),
ord_in_thread INT,
locations VARCHAR(255),
persons VARCHAR(255),
organizations VARCHAR(255),
external_links VARCHAR(255),
highlightText VARCHAR(255),
highlightTitle VARCHAR(255),
`language` VARCHAR(255),
title VARCHAR(255),
`text` VARCHAR(255),
published VARCHAR(255),
crawled VARCHAR(255)
);

CREATE TABLE thread_data (
master_uuid VARCHAR(255) PRIMARY KEY,
site_full VARCHAR(255),
main_image VARCHAR(255),
site_section VARCHAR(255),
section_title VARCHAR(255),
url VARCHAR(255),
country VARCHAR(255),
title VARCHAR(255),
performance_score INT,
site VARCHAR(255),
participants_count INT,
title_full VARCHAR(255),
spam_score FLOAT,
site_type VARCHAR(255),
published VARCHAR(255),
replies_count VARCHAR(255),
uuid VARCHAR(255)
);

CREATE TABLE social_data (
master_uuid VARCHAR(255) PRIMARY KEY,
gplus_shares INT,
pinterest_shares  INT,
vk_shares INT,
linkedin_shares INT,
facebook_likes INT,
facebook_shares INT,
facebook_comments INT,
stumbledupon_shares INT
);