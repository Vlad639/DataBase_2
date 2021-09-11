CREATE TABLE public."Humans" (
	"human_id" serial NOT NULL,
	"passport_number" character varying(10) NOT NULL,
	"second_name" character varying(50) NOT NULL,
	"first_name" character varying(50) NOT NULL,
	"last_name" character varying(50),
	"born_date" DATE NOT NULL,
	CONSTRAINT "Humans_pk" PRIMARY KEY ("human_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE public."Cities" (
	"city_id" serial NOT NULL,
	"city_name" character varying NOT NULL,
	CONSTRAINT "Cities_pk" PRIMARY KEY ("city_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE public."Streets" (
	"street_id" serial NOT NULL,
	"street_name" character varying NOT NULL,
	"city_link" integer NOT NULL,
	CONSTRAINT "Streets_pk" PRIMARY KEY ("street_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE public."Houses" (
	"house_id" serial NOT NULL,
	"house_number" character varying(10) NOT NULL,
	"street_link" integer NOT NULL,
	CONSTRAINT "Houses_pk" PRIMARY KEY ("house_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE public."Flats" (
	"flat_id" serial NOT NULL,
	"flat_number" integer NOT NULL,
	"house_link" integer NOT NULL,
	CONSTRAINT "Flats_pk" PRIMARY KEY ("flat_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE public."Residents" (
	"human_link" integer NOT NULL,
	"flat_link" integer NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE public."Flats_owners" (
	"human_link" integer NOT NULL,
	"flat_link" integer NOT NULL
) WITH (
  OIDS=FALSE
);





ALTER TABLE "Streets" ADD CONSTRAINT "Streets_fk0" FOREIGN KEY ("city_link") REFERENCES "Cities"("city_id");

ALTER TABLE "Houses" ADD CONSTRAINT "Houses_fk0" FOREIGN KEY ("street_link") REFERENCES "Streets"("street_id");

ALTER TABLE "Flats" ADD CONSTRAINT "Flats_fk0" FOREIGN KEY ("house_link") REFERENCES "Houses"("house_id");

ALTER TABLE "Residents" ADD CONSTRAINT "Residents_fk0" FOREIGN KEY ("human_link") REFERENCES "Humans"("human_id");
ALTER TABLE "Residents" ADD CONSTRAINT "Residents_fk1" FOREIGN KEY ("flat_link") REFERENCES "Flats"("flat_id");

ALTER TABLE "Flats_owners" ADD CONSTRAINT "Flats_owners_fk0" FOREIGN KEY ("human_link") REFERENCES "Humans"("human_id");
ALTER TABLE "Flats_owners" ADD CONSTRAINT "Flats_owners_fk1" FOREIGN KEY ("flat_link") REFERENCES "Flats"("flat_id");







