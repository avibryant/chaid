package com.avibryant.chaid

import scala.annotation.tailrec
import com.twitter.algebird.Semigroup

object FeatureMerger {
  def mergeUnordered[A](items : Seq[(A,(Int,Int))]) = mergeOrdered(items.sortBy{case (k,(g,b)) => g.toDouble / b})

  def mergeOrdered[A](items : Seq[(A,(Int,Int))]) = {
    merge(items){(counts : Seq[(Int, Int)]) =>
      if(counts.size <= 1) {
        1.0
      } else {
        val total = counts.map{case (g,b) => g+b}.sum.toDouble
        val gTotalRate = counts.map{case (g,b) => g}.sum.toDouble/total
        val bTotalRate = counts.map{case (g,b) => b}.sum.toDouble/total
        val expected = counts.flatMap{case (g,b) => List((g.toDouble + b.toDouble)*gTotalRate, (g.toDouble + b.toDouble)*bTotalRate)}
        val observed = counts.flatMap{case (g,b) => List(g,b)}
        val cs = observed.zip(expected).map{case (o,e) => (o-e)*(o-e)/e}.sum
        val csd = new org.apache.commons.math3.distribution.ChiSquaredDistribution(counts.length - 1)
        1 - csd.cumulativeProbability(cs)
      }
    }
  }

  def merge[A,B:Semigroup](items : Seq[(A,B)])(score : Seq[B] => Double) = {
    var initial = items.toList.map{case (a,b) => (List(a),b)}
    bestMerge(initial, initial, score(initial.map{_._2}))(score)
  }

  @tailrec
  def bestMerge[A,B:Semigroup](
    tail: List[(List[A], B)],
    bestOverall: List[(List[A], B)],
    bestOverallScore: Double,
    head: List[(List[A], B)] = Nil,
    bestCurrent: List[(List[A], B)] = Nil,
    bestPairScore: Double = 0.0)
    (score : Seq[B] => Double)
     : (List[List[A]], Double) = {
    (head, tail) match {
      case (Nil, Nil) => (Nil, 0.0)
      case (Nil, a :: Nil) => (bestOverall.map{_._1}, bestOverallScore)
      case (_, a :: Nil) => {
        val currentScore = score(bestCurrent.map{_._2})
        val (newBest, newBestScore) =
          if(currentScore < bestOverallScore)
            (bestCurrent, currentScore)
          else
            (bestOverall, bestOverallScore)
        bestMerge(bestCurrent, newBest, newBestScore)(score)
      }
      case (_, a :: b :: rest) => {
        val pairScore = score(List(a,b).map{_._2})
        val (newBestCurrent, newBestPairScore) =
          if(pairScore > bestPairScore)
            (((a._1 ++ b._1, Semigroup.plus(a._2, b._2)) :: head).reverse
              ++ rest,
            pairScore)
          else
            (bestCurrent, bestPairScore)
        bestMerge(b :: rest,
             bestOverall,
             bestOverallScore,
             a :: head,
             newBestCurrent,
             newBestPairScore)(score)
      }
    }
  }
}