from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from cassandra.cluster import Cluster
import tarfile, sys


def create_schemas(keyspace):

    cluster = Cluster(control_connection_timeout=600000000,connect_timeout=600000000)
    session = cluster.connect()
    session.default_timeout=600000000


    session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";")
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor' : 1 };")
    print("Created Keyspace: {!r}".format(keyspace))


    table="yelp_academic_dataset_business"

    session.execute("DROP TABLE IF EXISTS " + keyspace + "."+table+";")
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+ " ( \
            business_id text PRIMARY KEY, \
            name text, \
            neighborhood text,  \
            address text, \
            city text,  \
            state text,  \
            postal_code text,  \
            latitude text,  \
            longitude text,  \
            stars text,  \
            review_count text,  \
            is_open text,  \
            attributes text,  \
            categories list<text>,  \
            hours list<text>,  \
            type text );")
    print("Created Table: {!r}.{!r}".format(keyspace, table))

    table = "yelp_academic_dataset_review"

    session.execute("DROP TABLE IF EXISTS " + keyspace + "." + table + ";")
    session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( \
               review_id text PRIMARY KEY, \
               user_id text, \
               business_id text,  \
               stars text, \
               date text,  \
               text text,  \
               useful text,  \
               funny text,  \
               cool text,  \
               type text );")
    print("Created Table: {!r}.{!r}".format(keyspace, table))




    table = "yelp_academic_dataset_user"

    session.execute("DROP TABLE IF EXISTS " + keyspace + "." + table + ";")
    session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( \
               user_id text PRIMARY KEY, \
               name text, \
               review_count text,  \
               yelping_since text, \
               friends list<text>,  \
               useful text,  \
               funny text,  \
               cool text,  \
               fans text,  \
               elite list<text>,  \
               average_stars text,  \
               compliment_hot text,  \
               compliment_more text,  \
               compliment_profile text,  \
    compliment_cute text,  \
                 compliment_list text,  \
                 compliment_note text,  \
    compliment_plain text,  \
                 compliment_cool text,  \
                 compliment_funny text,  \
    compliment_writer text,  \
                 compliment_photos text,  \
               type text );")
    print("Created Table: {!r}.{!r}".format(keyspace, table))



    table = "yelp_academic_dataset_checkin"

    session.execute("DROP TABLE IF EXISTS " + keyspace + "." + table + ";")
    session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( \
               time list<text>,  \
               business_id text, \
               type text, \
    PRIMARY KEY (business_id,type));")

    print("Created Table: {!r}.{!r}".format(keyspace, table))



    table = "yelp_academic_dataset_tip"

    session.execute("DROP TABLE IF EXISTS " + keyspace + "." + table + ";")
    session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( \
               text text, \
               date text, \
               likes text,  \
               business_id text, \
               user_id text,  \
               type text, \
    PRIMARY KEY (text,date,likes,business_id,user_id,type));")
    print("Created Table: {!r}.{!r}".format(keyspace, table))




def run_driver(keyspace,spark,sqlContext):



    sqlContext = SQLContext(spark)


    df_business = sqlContext.read.json("yelp_academic_dataset_business.json")

    df_business.write \
   .format("org.apache.spark.sql.cassandra") \
   .mode('overwrite') \
   .options(table="yelp_academic_dataset_business", keyspace=keyspace) \
   .save()

    df_review = sqlContext.read.json("yelp_academic_dataset_review.json")

    df_review.write \
   .format("org.apache.spark.sql.cassandra") \
   .mode('overwrite') \
   .options(table="yelp_academic_dataset_review", keyspace=keyspace) \
   .save()

    df_user = sqlContext.read.json("yelp_academic_dataset_user.json")

    df_user.write \
   .format("org.apache.spark.sql.cassandra") \
   .mode('overwrite') \
   .options(table="yelp_academic_dataset_user", keyspace=keyspace) \
   .save()

    df_checkin = sqlContext.read.json("yelp_academic_dataset_checkin.json")

    df_checkin.write \
   .format("org.apache.spark.sql.cassandra") \
   .mode('overwrite') \
   .options(table="yelp_academic_dataset_checkin", keyspace=keyspace) \
   .save()

    df_tip = sqlContext.read.json("yelp_academic_dataset_tip.json")

    df_tip.write \
   .format("org.apache.spark.sql.cassandra") \
   .mode('overwrite') \
   .options(table="yelp_academic_dataset_tip", keyspace=keyspace) \
   .save()




def execute_queries(keyspace,spark,sqlContext):


    df_business =  spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="yelp_academic_dataset_business", keyspace=keyspace) \
         .load()

    df_review = spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="yelp_academic_dataset_review", keyspace=keyspace) \
         .load()

    df_user =  spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="yelp_academic_dataset_user", keyspace=keyspace) \
         .load()

    df_checkin =  spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="yelp_academic_dataset_checkin", keyspace=keyspace) \
         .load()

    df_tip =  spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="yelp_academic_dataset_tip", keyspace=keyspace) \
         .load()


    df_business.registerTempTable("business")
    df_review.registerTempTable("review")
    df_user.registerTempTable("user")
    df_checkin.registerTempTable("checkin")
    df_tip.registerTempTable("tip")


#Query No. 1

    sqlContext.sql("select u.user_id,u.name,count(r.review_id) number_of_reviews \
    from user u INNER JOIN review r ON u.user_id = r.user_id \
    group by u.user_id,u.name").show()

#Query No. 2
    sqlContext.sql("select distinct b.business_id,b.name,r.text,min(to_date(r.date)) \
                    over (partition by r.business_id) oldest_review_date_text, \
                    max(to_date(r.date)) over (partition by r.business_id) latest_review_date_text \
                    from business b inner join review r on b.business_id = r.business_id") \
              .show()

#Query No. 3
    sqlContext.sql("select distinct b.business_id,b.name,ck.time from business b inner join checkin ck \
                    ON b.business_id = ck.business_id").show()

#Query No. 4

    sqlContext.sql("select b.business_id,b.name,avg(t.likes) as avg_likes from business b inner join tip t \
                     ON b.business_id = t.business_id group by b.business_id,b.name having avg_likes > 3").show()



def untar(fname):
    if (fname.endswith(".tar")):
        tar = tarfile.open(fname)
        print(".....Yelp Dataset tar extraction in progress....")
        tar.extractall()
        tar.close()
        print("Yelp Dataset extracted in the Current Directory")
    else:
        print("Not a .tar file: '%s '" % sys.argv[0])




def main():
 if len(sys.argv) < 2:
        print("Usage: '%s yelpset_tar_file'" % sys.argv[0])
        sys.exit(0)
 else:
        untar(sys.argv[1])


 keyspace="yelp_dataset"

 spark = SparkSession.builder \
        .master("local") \
        .appName("Yelp Dataset Exploration") \
        .getOrCreate()
 sqlContext = SQLContext(spark)

 create_schemas(keyspace)
 run_driver(keyspace,spark,sqlContext)
 execute_queries(keyspace,spark,sqlContext)

 print("...Done...")




if __name__ == '__main__':
       main()
