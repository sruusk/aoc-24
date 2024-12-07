package advent

import org.apache.spark.util.AccumulatorV2

class BigIntAccumulator extends AccumulatorV2[BigInt, BigInt] {
  private var _sum: BigInt = BigInt(0)

  override def isZero: Boolean = _sum == 0

  override def copy(): AccumulatorV2[BigInt, BigInt] = {
    val newAcc = new BigIntAccumulator()
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = BigInt(0)
  }

  override def add(v: BigInt): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = {
    other match {
      case o: BigIntAccumulator => _sum += o._sum
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: BigInt = _sum
}
