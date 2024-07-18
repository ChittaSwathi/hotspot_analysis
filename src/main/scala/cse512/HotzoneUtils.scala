package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
      val rectangle = queryRectangle.split(",")
      val lat1 = rectangle(0).toDouble
      val long1 = rectangle(1).toDouble
      val lat2 = rectangle(2).toDouble
      val long2 = rectangle(3).toDouble

      val point = pointString.split(",")
      val x = point(0).toDouble
      val y = point(1).toDouble

      if ((x >= Math.min(lat1, lat2) && x <= Math.max(lat1, lat2)) && (y >= Math.min(long1, long2) && y <= Math.max(long1, long2)))
        {return true}

    return false;

  }
}
