package beam.utils.data.synthpop

import beam.utils.data.synthpop.models.Models
import com.typesafe.scalalogging.StrictLogging

object PopulationCorrection extends StrictLogging {

  def adjust(
    input: Seq[(Models.Household, Seq[Models.Person])]
  ): Map[Models.Household, Seq[Models.Person]] = {
    val finalResult = input.filter { case (_, persons) => persons.nonEmpty }.toMap
    logger.info(s"Read ${input.size} households with ${input.map(x => x._2.size).sum} people.")
    logger.info(
      s"After filtering empty households we got ${finalResult.size} households with ${finalResult.values.map(x => x.size).sum} people."
    )
    finalResult
  }
}
