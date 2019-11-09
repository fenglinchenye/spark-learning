package com.freedo.spark

/**
  * 类需要排序的 则需要继承 Ordered 覆盖compare方法
  * @param first
  * @param second
  */
class SecondarySortKey(val first:Int,val second:Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first-that.first!=0){
      this.first-that.first
    }else{
      this.second-that.second
    }
  }
}
