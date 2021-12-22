package beam.utils.data.synthpop

import beam.utils.data.synthpop.models.Models
import com.typesafe.scalalogging.StrictLogging

object PopulationCorrection extends StrictLogging {

  def adjust(
    input: Seq[(Models.Household, Seq[Models.Person])]
  ): Map[Models.Household, Seq[Models.Person]] = {
    // Take only with age is >= 16
    val allYears = input
      .map { case (hh, persons) =>
        hh -> persons
      }
      .filter { case (_, persons) => persons.nonEmpty }
      .toMap
    val removedHh = input.size - allYears.size
    val removedPeopleYoungerThan16 = input.map(x => x._2.size).sum - allYears.values.map(x => x.size).sum
    logger.info(s"Read ${input.size} households with ${input.map(x => x._2.size).sum} people")
    logger.info(s"""After filtering them got ${allYears.size} households with ${allYears.values
      .map(x => x.size)
      .sum} people.
         |Removed $removedHh households and $removedPeopleYoungerThan16 people who are younger than 16""".stripMargin)

    //    showAgeCounts(elderThan16Years)

    val finalResult = allYears.foldLeft(Map[Models.Household, Seq[Models.Person]]()) {
      case (acc, (hh: Models.Household, people)) =>
        if (people.isEmpty) acc
        else {
          acc + (hh -> people)
        }
    }
    val removedEmptyHh = allYears.size - finalResult.size
    val removedNonWorkers = allYears.map(x => x._2.size).sum - finalResult.values.map(x => x.size).sum
    logger.info(s"""After applying work force sampler got ${finalResult.size} households with ${finalResult.values
      .map(x => x.size)
      .sum} people.
         |Removed $removedEmptyHh households and $removedNonWorkers people""".stripMargin)

    finalResult
  }
}
