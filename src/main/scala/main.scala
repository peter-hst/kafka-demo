package hst.peter.kafka

import scala.annotation.tailrec

@main
def main(): Unit = {
  (1 to 300).foreach(_ => println(randStr(7, 3)))

}