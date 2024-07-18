package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " AND y >= " + minY + " AND z >= " + minZ + " AND x <= " + maxX + " AND y <= " + maxY + " AND z <= " + maxZ).orderBy("z", "y", "x")
  pickupInfo.createOrReplaceTempView("pickUpInfo")
//  pickupInfo.show()

  val dfPointsInCell = spark.sql("SELECT P.x, P.y, P.z, count(*) AS xCount FROM pickUpInfo AS P GROUP BY P.z, P.y, P.x ORDER BY P.z, P.y, P.x")
  dfPointsInCell.createOrReplaceTempView("CellsData")
//  dfPointsInCell.show()

  val X = spark.sql("SELECT SUM(CellsData.xCount) FROM CellsData").first().getLong(0).toDouble / (numCells*1.0)
  val sumX_pow2 = spark.sql("SELECT SUM(CellsData.xCount*CellsData.xCount) FROM CellsData").first().getLong(0).toDouble
  var S = Math.sqrt((sumX_pow2/numCells*1.0) - X*X)
  spark.udf.register("find_gscore", (sum_x: Double, W: Double) =>HotcellUtils.find_gscore(sum_x, W, X , S, numCells))

  spark.udf.register("isNeighborCell", (C1X: Int, C1Y: Int, C1Z: Int, C2X: Int, C2Y: Int, C2Z: Int) => HotcellUtils.isNeighborCell(C1X, C1Y, C1Z, C2X, C2Y, C2Z))
  val neighbor = spark.sql("SELECT C1.x, C1.y, C1.z, SUM(C2.xCount) as sum_x, COUNT(C2.xCount) as W FROM CellsData C1, CellsData C2 WHERE isNeighborCell(C1.x,C1.y,C1.z,C2.x,C2.y,C2.z) GROUP BY C1.x, C1.y, C1.z ")
  neighbor.createOrReplaceTempView("Neighbors")
//  neighbor.show()

  val gscore_50 = spark.sql("SELECT x, y, z from (SELECT x, y, z, find_gscore(sum_x, W) as score FROM Neighbors ORDER BY score DESC limit 50)")
  gscore_50.show()
  return gscore_50
  }
}