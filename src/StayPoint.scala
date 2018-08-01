import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat


object StayPoint {
  //标准时间格式转时间戳
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    dt.getTime()
  }


  //处理时间为标准格式
  def handle(line : String): String = {
    var str = ""
    val lineInfo = line.split(",")
    val Array(date, time) = lineInfo(3).split(" ")
    val dateInfo = date.split("-")
    val timeInfo = time.split(":")
    str += lineInfo(0) + "," + dateInfo(0) + dateInfo(1) + dateInfo(2) + timeInfo(0) +
      timeInfo(1) + timeInfo(2).substring(0, 2) + "," + lineInfo(4) + "," + lineInfo(5)
    return str
  }


  // 计算两点间的时间差
  def deltaTime(time1: String, time2: String): Long = {
    val t1 = tranTimeToLong(time1)
    val t2 = tranTimeToLong(time2)
    return (t1-t2).abs/1000
  }


  // 计算两点间的距离
  def distOfP2P(p1: List[Double], p2: List[Double]): Double = {
    val pointData = List(p1(0), p1(1), p2(0), p2(1)).map(data => math.toRadians(data))
    val dLng = math.abs(pointData(0) - pointData(2))
    val dLat = math.abs(pointData(1) - pointData(3))
    val a = math.pow(math.sin(dLat / 2), 2) + math.cos(pointData(1)) * math.cos(pointData(3)) * math.pow(math.sin(dLng / 2), 2)
    2 * math.asin(math.sqrt(a)) * 6371 * 1000
  }


  // 计算窗口内的平均距离
  def calculateAverDid(pointList: List[List[Double]]): Double = {
    val dataLength = pointList.length
    val averLng = pointList.map(point => point(0)).reduce((lng1, lng2) => lng1 + lng2) / dataLength
    val averLat = pointList.map(point => point(1)).reduce((lat1, lat2) => lat1 + lat2) / dataLength
    pointList.map(point => distOfP2P(point, List(averLng, averLat))).reduce((dis1, dis2) => dis1 + dis2) / dataLength
  }


  // 遍历所有点，查找POI
  def findPOI(traData: List[List[Double]], traTime: List[String], disHold: Int, pointHold: Int, timeHold: Int,
              falseHold: Int): Array[Int] = {
    val dataLength = traData.length
    var result = Array.fill(dataLength)(-2)

    // 循环初始化
    var clusterID = 0
    var i = 0
    var j = 1
    var falseCount = 0
    var falsePos = 0

    while (j < dataLength) {
      val deltaTimeOfPoint = deltaTime(traTime(j), traTime(j - 1))

      if (deltaTimeOfPoint <= timeHold) {
        // 点j满足时间阈值

        val calculateCluster = traData.dropRight(dataLength - j).drop(i)
        val averDis = calculateAverDid(calculateCluster)

        if (averDis <= disHold) {
          // 窗口i, j满足距离阈值
          j += 1
          falseCount = 0
          falsePos = 0
        }
        else {
          // 窗口i, j不满足距离阈值
          falseCount += 1
          if (falsePos == 0)
            falsePos = j

          if (falseCount <= falseHold)
          // 窗口i, j满足错误阈值
            j += 1

          else {
            val clusterLength = falsePos - i + 1
            if (clusterLength < pointHold) {
              // 窗口i, falsePos不满足数量阈值
              for (pos <- i to falsePos)
                result(pos) = -1
            } else {
              // 窗口i, falsePos满足数量阈值
              for (pos <- i to falsePos)
                result(pos) = clusterID
              clusterID += 1
            }
            i = falsePos + 1
            j = i + 1
            falsePos = 0
            falseCount = 0
          }
        }
      } else {
        // 点j不满足时间阈值
        falseCount += 1
        if (falsePos == 0)
          falsePos = j
        if (falseCount <= falseHold)
          j += 1
        else {
          val clusterLength = falsePos - i + 1
          if (clusterLength < pointHold) {
            // 窗口i, falsePos不满足数量阈值
            for (pos <- i to falsePos)
              result(pos) = -1
          } else {
            for (pos <- i to falsePos)
              result(pos) = clusterID
            clusterID += 1
          }
          i = falsePos + 1
          j = i + 1
          falseCount = 0
          falsePos = 0
        }
      }
    }

    val clusterLength = j - i + 1
    if (clusterLength < pointHold) {
      for (pos <- i to dataLength - 1)
        result(pos) = -1
    } else {
      for (pos <- i to dataLength - 1)
        result(pos) = clusterID
    }
    return result
  }


  def main(args: Array[String]) {
    //为每一行添加一个key
    def addkey(str : String) : (String,String) = {
      val key = str.split(',')(0)
      return ((key,str))
    }

    //聚类
    def clustering(tuple:(String,Iterable[String])): List[Array[(Int, Array[String])]] = {
      val traData = new ArrayBuffer[Array[String]]()
      tuple._2.foreach(line=>{traData += line.split(',')})
      val traTime = traData.map(attributrs => attributrs(1)).toList
      val traLngLat = traData.map(attributes => List(attributes(2).toDouble, attributes(3).toDouble)).toList

      val clusterArgs = args(0).split(',').map(_.toInt)
      val clusterResult = findPOI(traLngLat, traTime, clusterArgs(0), clusterArgs(1), clusterArgs(2), clusterArgs(3))
      clusterResult.zip(traData).groupBy(line => line._1).map(cluster => {
        cluster._2.sortBy(point => {
          point._2(1)
        })
      }).toList.sortBy(tra => {
        tra(0)._1
      })
    }

    def getClusterInfo(cluster: Array[(Int, Array[String])]): String = {
      if (cluster(0)._1 != -1) {
        val carID = cluster(0)._2(0)
        val clusterDate = cluster(0)._2(1).substring(0, 8)
        val clusterLength = cluster.length
        val clusterLng = (cluster.map(p => p._2(2).toDouble).sum / clusterLength).toString
        val clusterLat = (cluster.map(p => p._2(3).toDouble).sum / clusterLength).toString
        val clusterStartTime = cluster(0)._2(1).substring(8, 14)
        val clusterEndTime = cluster(clusterLength - 1)._2(1).substring(8, 14)
        val clusterDTime = deltaTime(cluster(0)._2(1), cluster(cluster.length - 1)._2(1))
        var clusterPointInfo = ""
        cluster.foreach(p => {
          clusterPointInfo += p._2(1) + "," + p._2(2) + "," + p._2(3) + "\t"
        })
        carID + "," + clusterDate + "," + clusterLng + "," + clusterLat + "," + clusterStartTime + "," +
          clusterEndTime + "," + clusterDTime + "," + clusterLength + "\t" + clusterPointInfo + "\n"
      } else {
        ""
      }
    }

    val conf = new SparkConf().setAppName("StayPoint2")//.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(1))//轨迹文件路径
    val input_ = input.map(line => handle(line)).sortBy(line => line.split(",")(1))
    val groupedInput = input_.map(line => addkey(line)).groupByKey()
//    groupedInput.map(clustering).map(tra => {
//      var outputStr = ""
//      tra.foreach(cluster => {
//        outputStr += getClusterInfo(cluster)
//      })
//      outputStr
//    }).filter(str => str != "").repartition(1).saveAsTextFile(args(2))
    groupedInput.map(tra => {
      val poiResult = clustering(tra)
      var outputStr = ""
      poiResult.foreach(cluster => {
        outputStr += getClusterInfo(cluster)
      })
      if (outputStr != "")
        outputStr.dropRight(1)
      else
        outputStr
    }).filter(str => str != "").repartition(1).saveAsTextFile(args(2))
  }
}
