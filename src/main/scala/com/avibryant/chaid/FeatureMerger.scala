package com.avibryant.chaid

import scala.annotation.tailrec
import org.apache.commons.math3.distribution.ChiSquaredDistribution

object FeatureMerger {

  def mergeUnordered[A](counts : List[(A,Int,Int)]) =
    mergeOrdered(counts.sortBy{case (k,g,b) => g.toDouble / b})

  def mergeOrdered[A](counts : List[(A,Int,Int)]) = {
    var initial = counts.map{case (k,g,b) => (List(k), g, b)}
    bestMerge(initial, initial, score(initial))
  }

  @tailrec
  def bestMerge[A](
    tail: List[(List[A], Int, Int)],
    bestOverall: List[(List[A], Int, Int)],
    bestOverallScore: Double,
    head: List[(List[A], Int, Int)] = Nil,
    bestCurrent: List[(List[A], Int, Int)] = Nil,
    bestPairScore: Double = 0.0) : (List[List[A]], Double) = {
    (head, tail) match {
      case (Nil, Nil) => (Nil, 0.0)
      case (Nil, a :: Nil) => (bestOverall.map{_._1}, bestOverallScore)
      case (_, a :: Nil) => {
        val currentScore = score(bestCurrent)
        val (newBest, newBestScore) =
          if(currentScore < bestOverallScore)
            (bestCurrent, currentScore)
          else
            (bestOverall, bestOverallScore)
        bestMerge(bestCurrent, newBest, newBestScore)
      }
      case (_, a :: b :: rest) => {
        val pairScore = score(List(a,b))
        val (newBestCurrent, newBestPairScore) =
          if(pairScore > bestPairScore)
            (((a._1 ++ b._1, a._2 + b._2, a._3 + b._3) :: head).reverse
              ++ rest,
            pairScore)
          else
            (bestCurrent, bestPairScore)
        bestMerge(b :: rest,
             bestOverall,
             bestOverallScore,
             a :: head,
             newBestCurrent,
             newBestPairScore)
      }
    }
  }

  def score[A](counts : List[(List[A], Int, Int)]) : Double = {
    if(counts.size <= 1)
      return 1.0

    val total = counts.map{case (k,g,b) => g+b}.sum.toDouble
    val gTotalRate = counts.map{case (k,g,b) => g}.sum.toDouble/total
    val bTotalRate = counts.map{case (k,g,b) => b}.sum.toDouble/total
    val expected = counts.flatMap{case (k,g,b) => List((g.toDouble + b.toDouble)*gTotalRate, (g.toDouble + b.toDouble)*bTotalRate)}
    val observed = counts.flatMap{case (k,g,b) => List(g,b)}
    val cs = observed.zip(expected).map{case (o,e) => (o-e)*(o-e)/e}.sum
    val csd = new ChiSquaredDistribution(counts.length - 1)
    1 - csd.cumulativeProbability(cs)
  }
}