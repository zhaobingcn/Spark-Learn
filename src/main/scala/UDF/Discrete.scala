package UDF

trait Discrete {

  def discrete_0_INF(N : Int = 2) = {
    x : Double => math.round(math.log(x + 1) * (math.pow(10, N)).toInt ) / math.pow(10, N)
  }

  def discrete_0_1(N : Int = 2) = {
    x : Double => math.round(- math.log(1.0002 / (0.0001 + x) - 1) * (math.pow(10, N)).toInt ) / math.pow(10, N)
  }

  def discrete_NINF_INF(N : Int = 2) = {
    x : Double => {
      if(x >= 0){
        math.round(math.log(x + 1) * (math.pow(10, N)).toInt ) / math.pow(10, N)
      }
      else {
        -math.round(math.log(x + 1) * (math.pow(10, N)).toInt ) / math.pow(10, N)
      }
    }
  }
}

//object Discrete{
//  def apply: Discrete = new Discrete()
//}

