import Example.{Word, WordCountState}
import FruitCount.{ Fruit, FruitCountState }
import org.apache.spark.sql.streaming.GroupState

object StateHelper {

  val MappingFunction: (String, Iterator[Word], GroupState[Set[WordCountState]]) => Iterator[WordCountState] = (key, values, state) => {
    if(state.hasTimedOut){
      println("State Timed out")
      state.remove()
      state.getOption.getOrElse(Seq.empty[WordCountState]).toIterator
    }else {
      val currentState = state.getOption
      val updatedState = currentState match {
        case Some(s) =>
          println("Printing existing state...")
          state.get.foreach(println(_))
          val countValue: Long = values.size
          val newStateValue: Set[WordCountState] = state.get.map{ s =>
            if(s.value == key){
              s.copy(count = s.count + countValue)
            }
            else s
          }
          newStateValue
        case _ =>
          println("No state")
          Set(WordCountState(key,values.size))
      }
      state.update(updatedState)
      state.setTimeoutDuration("1 minute")
      updatedState.toIterator
    }
  }

  val fruitMappingFunction: (String, Iterator[Fruit], GroupState[Set[FruitCountState]]) => Iterator[Fruit] = (key, values, state) => {
    if(state.hasTimedOut){
      println("State Timed out")
      state.remove()
      values
    }else {
      val incomingValues = values.toSeq
      val currentState = state.getOption
      val updatedState = currentState match {
        case Some(s) =>
          println("Printing existing state...")
          state.get.foreach(println(_))
          val countValue: Long = incomingValues.size
          println("Value size "+countValue)
          val newStateValue: Set[FruitCountState] = state.get.map{ s =>
            if(s.fruit == key){
              s.copy(count = s.count + countValue)
            }
            else s
          }
          newStateValue
        case _ =>
          println("No state")
          Set(FruitCountState(key,incomingValues.size))
      }
      state.update(updatedState)
      state.setTimeoutDuration("5 minute")
      incomingValues.toIterator
    }
  }
}
