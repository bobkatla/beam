package beam.agentsim.infrastructure.parking

import beam.agentsim.agents.vehicles.VehicleCategory.VehicleCategory
import beam.agentsim.agents.vehicles.VehicleManager.ReservedFor
import beam.agentsim.agents.vehicles.{VehicleCategory, VehicleManager}
import beam.agentsim.infrastructure.charging.ChargingPointType
import beam.agentsim.infrastructure.parking.ParkingZoneSearch.ZoneSearchTree
import beam.agentsim.infrastructure.power.SitePowerManager
import beam.agentsim.infrastructure.taz.TAZ
import beam.sim.BeamServices
import beam.sim.config.BeamConfig
import beam.utils.csv.GenericCsvReader
import beam.utils.logging.ExponentialLazyLogging
import beam.utils.matsim_conversion.MatsimPlanConversion.IdOps
import beam.utils.{FileUtils, MathUtils, OutputDataDescriptor, OutputDataDescriptorObject}
import org.apache.commons.lang3.StringUtils.isBlank
import org.matsim.api.core.v01.{Coord, Id}
import org.matsim.core.network.NetworkUtils
import org.matsim.core.utils.io.IOUtils
import org.matsim.households.HouseholdUtils

import java.io.{BufferedReader, File, IOException}
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

// utilities to read/write parking zone information from/to a file
object ParkingZoneFileUtils extends ExponentialLazyLogging {

  type jMap = java.util.Map[String, String]

  /**
    * header for parking files (used for writing new parking files)
    */
  val ParkingFileHeader: String = List(
    "taz",
    "parkingType",
    "pricingModel",
    "chargingPointType",
    "numStalls",
    "feeInCents",
    "reservedFor",
    "timeRestrictions",
    "parkingZoneId",
    "locationX",
    "locationY",
    "sitePowerManager",
    "energyStorageCapacityInKWh",
    "energyStorageSOC"
  ).mkString(",")

  /**
    * when a parking file is not provided, we generate one that covers all TAZs with free and ubiquitous parking
    * this should consider charging when it is implemented as well.
    * @param geoId a valid id for a geo object
    * @param parkingType the parking type we are using to generate a row
    * @param maybeChargingPoint charging point type
    * @return a row describing infinite free parking at this TAZ
    */
  def defaultParkingRow(
    geoId: Id[TAZ],
    parkingType: ParkingType,
    maybeChargingPoint: Option[ChargingPointType],
    defaultReservedFor: ReservedFor
  ): String =
    List(
      geoId.toString, // taz
      parkingType.toString, // parkingType
      PricingModel.FlatFee(0).toString, // pricingModel
      maybeChargingPoint.map(_.toString).getOrElse("NoCharger"), // chargingPointType
      ParkingZone.UbiqiutousParkingAvailability.toString, // numStalls
      "0", // feeInCents
      defaultReservedFor.toString, // reservedFor
      "", // timeRestrictions
      "", // parkingZoneId
      "", // locationX
      "", // locationY
      "", // sitePowerManager
      "", // energyStorageCapacityInKWh
      "" // energyStorageSOC
    ).mkString(",")

  /**
    * used to build up parking alternatives from a file
    * @param zones the parking zones read in
    * @param tree the search tree constructed from the loaded zones
    * @param totalRows number of rows read
    * @param failedRows number of rows which failed to parse
    */
  case class ParkingLoadingAccumulator(
    zones: mutable.Map[Id[ParkingZoneId], ParkingZone] = mutable.Map.empty[Id[ParkingZoneId], ParkingZone],
    tree: mutable.Map[Id[TAZ], Map[ParkingType, Vector[Id[ParkingZoneId]]]] =
      mutable.Map.empty[Id[TAZ], Map[ParkingType, Vector[Id[ParkingZoneId]]]],
    totalRows: Int = 0,
    failedRows: Int = 0
  ) {

    def countFailedRow: ParkingLoadingAccumulator =
      this.copy(
        totalRows = totalRows + 1,
        failedRows = failedRows + 1
      )

    def someRowsFailed: Boolean = failedRows > 0

    def totalParkingStalls: Long = zones.map { _._2.maxStalls.toLong }.sum

    def parkingStallsPlainEnglish: String = {
      val count: Long = totalParkingStalls
      if (count > 1000000000L) s"${count / 1000000000L} billion"
      else if (count > 1000000L) s"${count / 1000000L} million"
      else if (count > 1000L) s"${count / 1000L} thousand"
      else count.toString
    }

  }

  /**
    * parking data associated with a row of the parking file
    * @param tazId a TAZ id
    * @param parkingType the parking type of this row
    * @param parkingZone the parking zone produced by this row
    */
  case class ParkingLoadingDataRow(tazId: Id[TAZ], parkingType: ParkingType, parkingZone: ParkingZone)

  /**
    * write the loaded set of parking and charging options to an instance parking file
    *
    * @param stallSearch the search tree of available parking options
    * @param stalls the stored ParkingZones
    * @param writeDestinationPath a file path to write to
    */
  def writeParkingZoneFile(
    stallSearch: ZoneSearchTree[TAZ],
    stalls: Map[Id[ParkingZoneId], ParkingZone],
    writeDestinationPath: String
  ): Unit = {

    val destinationFile = new File(writeDestinationPath)

    Try {
      for {
        (tazId, parkingTypesSubtree)  <- stallSearch.toList
        (parkingType, parkingZoneIds) <- parkingTypesSubtree.toList
        parkingZoneId                 <- parkingZoneIds
      } yield {

        val parkingZone = stalls(parkingZoneId)
        val (pricingModel, feeInCents) = parkingZone.pricingModel match {
          case None     => ("", "")
          case Some(pm) => (s"$pm", s"${pm.costInDollars * 100.0}")
        }
        val chargingPoint = parkingZone.chargingPointType match {
          case None     => "NoCharger"
          case Some(cp) => s"$cp"
        }
        val reservedFor = parkingZone.reservedFor.toString
        val timeRestrictions = parkingZone.timeRestrictions.map(toString).mkString("|")
        val parkingZoneIdStr = parkingZone.parkingZoneId.toString
        val (locationXStr, locationYStr) =
          parkingZone.link.map(link => (link.getCoord.getX.toString, link.getCoord.getY.toString)).getOrElse(("", ""))
        List(
          tazId.toString,
          parkingType.toString,
          pricingModel,
          chargingPoint,
          parkingZone.maxStalls,
          feeInCents,
          reservedFor,
          timeRestrictions,
          parkingZoneIdStr,
          locationXStr,
          locationYStr
        ).mkString(",")
      }
    } match {
      case Failure(e) =>
        throw new RuntimeException(s"failed while converting parking configuration to csv format.\n$e")
      case Success(rows) =>
        val newlineFormattedCSVOutput: String = (List(ParkingFileHeader) ::: rows).mkString("\n")
        Try {
          destinationFile.getParentFile.mkdirs()
          val writer = IOUtils.getBufferedWriter(writeDestinationPath)
          writer.write(newlineFormattedCSVOutput)
          writer.close()
        } match {
          case Failure(e) =>
            throw new IOException(s"failed while writing parking configuration to file $writeDestinationPath.\n$e")
          case Success(_) =>
        }
    }
  }

  /**
    * loads taz parking data from file, creating an array of parking zones along with a search tree to find zones.
    *
    * the Array[ParkingZone] should be a private member of at most one Actor to prevent race conditions.
    *
    * @param filePath location in FS of taz parking data file (.csv)
    * @return table and tree
    */
  def fromFile(
    filePath: String,
    rand: Random,
    beamConfig: Option[BeamConfig],
    beamServices: Option[BeamServices],
    parkingStallCountScalingFactor: Double = 1.0,
    parkingCostScalingFactor: Double = 1.0
  ): (Map[Id[ParkingZoneId], ParkingZone], ZoneSearchTree[TAZ]) = {
    val parkingLoadingAccumulator =
      fromFileToAccumulator(
        filePath,
        rand,
        beamConfig,
        beamServices,
        parkingStallCountScalingFactor,
        parkingCostScalingFactor
      )
    (parkingLoadingAccumulator.zones.toMap, parkingLoadingAccumulator.tree)
  }

  def fromFiles(
    filePath: String,
    rand: Random,
    beamConfig: Option[BeamConfig],
    beamServices: Option[BeamServices],
    parkingStallCountScalingFactor: Double = 1.0,
    parkingCostScalingFactor: Double = 1.0
  ): (Map[Id[ParkingZoneId], ParkingZone], ZoneSearchTree[TAZ]) = {
    val parkingLoadingAccumulator =
      fromFileToAccumulator(
        filePath,
        rand,
        beamConfig,
        beamServices,
        parkingStallCountScalingFactor,
        parkingCostScalingFactor
      )
    (parkingLoadingAccumulator.zones.toMap, parkingLoadingAccumulator.tree)
  }

  /**
    * Loads taz parking data from file, creating a parking zone accumulator
    * This method allows to read multiple parking files into a single array of parking zones
    * along with a single search tree to find zones
    *
    * @param filePath location in FS of taz parking data file (.csv)
    * @return parking zone accumulator
    */
  def fromFileToAccumulator(
    filePath: String,
    rand: Random,
    beamConfig: Option[BeamConfig],
    beamServices: Option[BeamServices],
    parkingStallCountScalingFactor: Double = 1.0,
    parkingCostScalingFactor: Double = 1.0,
    parkingLoadingAcc: ParkingLoadingAccumulator = ParkingLoadingAccumulator()
  ): ParkingLoadingAccumulator = {
    FileUtils.using(FileUtils.getReader(filePath)) { reader =>
      Try(
        fromBufferedReader(
          reader,
          rand,
          beamConfig,
          beamServices,
          parkingStallCountScalingFactor,
          parkingCostScalingFactor,
          parkingLoadingAcc
        )
      ) match {
        case Success(parkingLoadingAccumulator) =>
          logger.info(
            s"loaded ${parkingLoadingAccumulator.totalRows} rows as parking zones from $filePath, with ${parkingLoadingAccumulator.parkingStallsPlainEnglish} stalls (${parkingLoadingAccumulator.totalParkingStalls}) in system"
          )
          if (parkingLoadingAccumulator.someRowsFailed) {
            logger.warn(s"${parkingLoadingAccumulator.failedRows} rows of parking data failed to load")
          }
          parkingLoadingAccumulator
        case Failure(e) =>
          logger.error("Failure", e)
          throw new java.io.IOException(s"Unable to load parking configuration file with path $filePath.\n$e")
      }
    }
  }

  /**
    * loads taz parking data from file, creating a lookup table of stalls along with a search tree to find stalls
    *
    * @param reader a java.io.BufferedReader of a csv file
    * @return ParkingZone array and tree lookup
    */
  def fromBufferedReader(
    reader: BufferedReader,
    rand: Random,
    beamConfig: Option[BeamConfig],
    beamServices: Option[BeamServices],
    parkingStallCountScalingFactor: Double = 1.0,
    parkingCostScalingFactor: Double = 1.0,
    parkingLoadingAccumulator: ParkingLoadingAccumulator = ParkingLoadingAccumulator()
  ): ParkingLoadingAccumulator = {

    val (iterator, closable) = GenericCsvReader.readFromReaderAs[jMap](reader, identity)
    @tailrec
    def _read(
      accumulator: ParkingLoadingAccumulator
    ): ParkingLoadingAccumulator = {
      if (iterator.hasNext) {
        val csvRow = iterator.next()
        val updatedAccumulator = parseParkingZoneFromRow(
          csvRow,
          accumulator.totalRows,
          rand,
          beamConfig,
          beamServices,
          None,
          parkingStallCountScalingFactor,
          parkingCostScalingFactor
        ) match {
          case Some(row: ParkingLoadingDataRow) if row.parkingZone.stallsAvailable > 0 =>
            // After sampling down parking certain parking zone became unavailable. We keep only available ones.
            addStallToSearch(row, accumulator)
          case _ =>
            accumulator.countFailedRow
        }
        _read(updatedAccumulator)
      } else {
        accumulator
      }
    }

    try {
      _read(parkingLoadingAccumulator)
    } finally {
      closable.close()
    }
  }

  /**
    * loads taz parking data from file, creating a lookup table of stalls along with a search tree to find stalls
    *
    * @param csvFileContents each line from a file to be read
    * @return table and search tree
    */
  def fromIterator(
    csvFileContents: Iterator[String],
    beamConfig: Option[BeamConfig],
    beamServices: Option[BeamServices],
    random: Random = Random,
    defaultReservedFor: Option[ReservedFor] = None,
    parkingStallCountScalingFactor: Double = 1.0,
    parkingCostScalingFactor: Double = 1.0,
    parkingLoadingAcc: ParkingLoadingAccumulator = ParkingLoadingAccumulator()
  ): ParkingLoadingAccumulator = {

    val withLineBreaks = csvFileContents.filterNot(_.trim.isEmpty).flatMap(x => Seq(x, "\n"))
    val (iterator, closable) =
      GenericCsvReader.readFromReaderAs[jMap](FileUtils.readerFromIterator(withLineBreaks), identity)

    try {
      iterator.foldLeft(parkingLoadingAcc) { (accumulator, csvRow) =>
        Try {
          parseParkingZoneFromRow(
            csvRow,
            accumulator.totalRows,
            random,
            beamConfig,
            beamServices,
            defaultReservedFor,
            parkingStallCountScalingFactor,
            parkingCostScalingFactor
          ) match {
            case Some(row: ParkingLoadingDataRow) if row.parkingZone.stallsAvailable > 0 =>
              // After sampling down parking certain parking zone became unavailable. We keep only available ones.
              addStallToSearch(row, accumulator)
            case _ =>
              accumulator.countFailedRow
          }
        } match {
          case Success(updatedAccumulator) =>
            updatedAccumulator
          case Failure(e) =>
            logger.info(s"failed to load parking data row due to ${e.getMessage}. Original row: '$csvRow'")
            accumulator.countFailedRow
        }
      }
    } finally {
      closable.close()
    }
  }

  private val TimeRestriction = """(\w+)\|(\d{1,2})(?::(\d{2}))?-(\d{1,2})(?::(\d{2}))?""".r

  private[parking] def parseTimeRestrictions(timeRestrictionsString: String): Map[VehicleCategory, Range] = {

    def parseTimeRestriction(timeRestrictionString: String): Option[(VehicleCategory, Range)] = {
      timeRestrictionString match {
        case TimeRestriction(
              categoryStr,
              hour1,
              minute1,
              hour2,
              minute2
            ) =>
          val category = VehicleCategory.fromString(categoryStr)
          val from = hour1.toInt * 3600 + Option(minute1).map(_.toInt).getOrElse(0) * 60
          val to = hour2.toInt * 3600 + Option(minute2).map(_.toInt).getOrElse(0) * 60
          Some(category -> Range(from, to))
        case _ =>
          logger.error(s"Cannot parse time restriction data: $timeRestrictionString")
          None
      }
    }

    // values look like LightDutyTruck:00:00-14:00|Car:14:00-18:00|Bike:18:00-24:00
    Option(timeRestrictionsString)
      .getOrElse("")
      .split(';')
      .map(_.trim)
      .filterNot(_.isEmpty)
      .flatMap(parseTimeRestriction)
      .toMap

  }

  private def toString(restriction: (VehicleCategory, Range)): String = {
    val (category, range) = restriction
    val fromHour = range.start / 3600
    val fromMin = range.start % 3600 / 60
    val toHour = range.end / 3600
    val toMin = range.end % 3600 / 60
    "%s:%d:%02d-%d:%02d".format(category, fromHour, fromMin, toHour, toMin)
  }

  private def getHouseholdLocation(beamServices: BeamServices, houseoldId: Id[_]): Option[Coord] = {
    Try {
      val household = beamServices.matsimServices.getScenario.getHouseholds.getHouseholds.get(houseoldId)
      val x = HouseholdUtils.getHouseholdAttribute(household, "homecoordx").asInstanceOf[Double]
      val y = HouseholdUtils.getHouseholdAttribute(household, "homecoordy").asInstanceOf[Double]
      new Coord(x, y)
    } match {
      case Success(coord) => Some(coord)
      case Failure(e) =>
        logger.warn(s"Household without coordinate! $e")
        None
    }
  }

  /**
    * parses a row of parking configuration into the data structures used to represent it
    *
    * @param csvRow the comma-separated parking attributes
    * @return a ParkingZone and it's corresponding ParkingType and Taz Id
    */
  def parseParkingZoneFromRow(
    csvRow: jMap,
    rowNumber: Int,
    rand: Random,
    beamConfig: Option[BeamConfig],
    beamServices: Option[BeamServices],
    defaultReservedFor: Option[ReservedFor] = None,
    parkingStallCountScalingFactor: Double = 1.0,
    parkingCostScalingFactor: Double = 1.0
  ): Option[ParkingLoadingDataRow] = {
    if (!validateCsvRow(csvRow)) {
      logger.error(s"Failed to match row of parking configuration '$csvRow' to expected schema")
      return None
    }
    implicit val randImplicit: Random = rand
    val tazString = csvRow.get("taz")
    val parkingTypeString = csvRow.get("parkingType")
    val pricingModelString = csvRow.get("pricingModel")
    val chargingTypeString = csvRow.get("chargingPointType")
    val numStallsString = csvRow.get("numStalls")
    val feeInCentsString = csvRow.get("feeInCents")
    val reservedForString = csvRow.get("reservedFor")
    val timeRestrictionsString = csvRow.get("timeRestrictions")
    val parkingZoneIdString = csvRow.get("parkingZoneId")
    val locationXString = csvRow.get("locationX")
    val locationYString = csvRow.get("locationY")
    val sitePowerManagerString = csvRow.get("sitePowerManager")
    val energyStorageCapacityString = csvRow.get("energyStorageCapacityInKWh")
    val energyStorageSOCString = csvRow.get("energyStorageSOC")
    Try {
      val feeInCents = feeInCentsString.toDouble
      val newCostInDollarsString = (feeInCents * parkingCostScalingFactor / 100.0).toString
      val reservedFor = validateReservedFor(reservedForString, beamConfig, defaultReservedFor)
      // parse this row from the source file
      val taz = tazString.toUpperCase.createId[TAZ]
      val parkingType = ParkingType(parkingTypeString)
      val pricingModel = PricingModel(pricingModelString, newCostInDollarsString)
      val timeRestrictions = parseTimeRestrictions(timeRestrictionsString)
      val chargingPoint = ChargingPointType(chargingTypeString)
      val numStalls = calculateNumStalls(numStallsString.toDouble, reservedFor, parkingStallCountScalingFactor)
      val parkingZoneIdMaybe =
        if (isBlank(parkingZoneIdString)) Some(ParkingZone.createId(rowNumber.toString))
        else Some(ParkingZone.createId(parkingZoneIdString))
      val linkMaybe = !isBlank(locationXString) && !isBlank(locationYString) match {
        case true if beamServices.isDefined =>
          val coord = new Coord(locationXString.toDouble, locationYString.toDouble)
          Some(NetworkUtils.getNearestLink(beamServices.get.beamScenario.network, beamServices.get.geo.wgs2Utm(coord)))
        case false if beamServices.isDefined && reservedFor.managerType == VehicleManager.TypeEnum.Household =>
          getHouseholdLocation(beamServices.get, reservedFor.managerId) map { homeCoord =>
            NetworkUtils.getNearestLink(
              beamServices.get.beamScenario.network,
              homeCoord
            )
          }
        case _ => None
      }
      val sitePowerManagerMaybe = if (isBlank(sitePowerManagerString)) None else Some(sitePowerManagerString)
      val energyStorageCapacityMaybe =
        if (isBlank(energyStorageCapacityString)) None else Some(energyStorageCapacityString.toDouble)
      val energyStorageSOCMaybe = if (isBlank(energyStorageSOCString)) None else Some(energyStorageSOCString.toDouble)
      val parkingZone =
        ParkingZone.init(
          parkingZoneIdMaybe,
          taz,
          parkingType,
          reservedFor,
          numStalls,
          chargingPoint,
          pricingModel,
          timeRestrictions,
          linkMaybe,
          sitePowerManagerMaybe,
          energyStorageCapacityMaybe,
          energyStorageSOCMaybe
        )
      ParkingLoadingDataRow(taz, parkingType, parkingZone)
    } match {
      case Success(updatedAccumulator) =>
        Some { updatedAccumulator }
      case Failure(e) =>
        throw new java.io.IOException(s"Failed to load parking data from row with contents '$csvRow'.", e)
    }
  }

  private def calculateNumStalls(initialNumStalls: Double, reservedFor: ReservedFor, scalingFactor: Double)(implicit
    rand: Random
  ): Int = {
    reservedFor.managerType match {
      case VehicleManager.TypeEnum.Household =>
        initialNumStalls.toInt
      case _ =>
        val expectedNumberOfStalls = initialNumStalls * scalingFactor
        MathUtils.roundUniformly(expectedNumberOfStalls, rand).toInt
    }
  }

  private def validateReservedFor(
    reservedForString: String,
    beamConfigMaybe: Option[BeamConfig],
    defaultReservedFor: Option[ReservedFor] = None
  ): ReservedFor = {
    VehicleManager.createOrGetReservedFor(reservedForString, beamConfigMaybe) match {
      case Some(reservedFor)                   => reservedFor
      case None if defaultReservedFor.nonEmpty => defaultReservedFor.get
      case _ =>
        logger.warn(
          s"The following reservedFor value $reservedForString in parking file " +
          s"does not correspond to any known vehicle managers as predefined in the config file." +
          s"falling back to default manager"
        )
        VehicleManager.AnyManager
    }
  }

  private def validateCsvRow(csvRow: jMap): Boolean = {
    val allRequiredPresented = Seq("taz", "parkingType", "pricingModel", "chargingPointType", "numStalls", "feeInCents")
      .forall(key => {
        val value = csvRow.get(key)
        value != null && value.nonEmpty
      })
    allRequiredPresented &&
    Try(csvRow.get("numStalls").toDouble).toOption.exists(_ >= 0) &&
    Try(csvRow.get("feeInCents").toDouble).toOption.exists(_ >= 0)
  }

  /**
    * a kind of lens-based update for the search tree
    *
    * @param row the row data we parsed from a file
    * @param accumulator the currently loaded zones and search tree
    * @return updated tree, stalls
    */
  private[ParkingZoneFileUtils] def addStallToSearch(
    row: ParkingLoadingDataRow,
    accumulator: ParkingLoadingAccumulator
  ): ParkingLoadingAccumulator = {

    // find any data stored already within this TAZ and with this ParkingType
    val parkingTypes = accumulator.tree.getOrElse(row.tazId, Map())
    val parkingZoneIds: Vector[Id[ParkingZoneId]] =
      parkingTypes.getOrElse(row.parkingType, Vector.empty[Id[ParkingZoneId]])

    // create new ParkingZone in array with new parkingZoneId. should this be an ArrayBuilder?
    accumulator.zones.put(row.parkingZone.parkingZoneId, row.parkingZone)

    // update the tree with the id of this ParkingZone
    accumulator.tree.put(
      row.tazId,
      parkingTypes.updated(
        row.parkingType,
        (parkingZoneIds :+ row.parkingZone.parkingZoneId).sorted
      )
    )

    ParkingLoadingAccumulator(accumulator.zones, accumulator.tree, accumulator.totalRows + 1, accumulator.failedRows)
  }

  /**
    * generates ubiquitous parking from a taz centers file, such as test/input/beamville/taz-centers.csv
    * @param geoObjects geo objects that should be used to hold parking stalls
    * @param parkingTypes the parking types we are generating, by default, the complete set
    * @return
    */
  def generateDefaultParkingFromGeoObjects(
    geoObjects: Iterable[TAZ],
    random: Random,
    defaultReservedFor: ReservedFor,
    parkingTypes: Seq[ParkingType] = ParkingType.AllTypes
  ): (Map[Id[ParkingZoneId], ParkingZone], ZoneSearchTree[TAZ]) = {
    val parkingLoadingAccumulator =
      generateDefaultParkingAccumulatorFromGeoObjects(
        geoObjects,
        random,
        defaultReservedFor,
        parkingTypes
      )
    (parkingLoadingAccumulator.zones.toMap, parkingLoadingAccumulator.tree)
  }

  /**
    * generates ubiquitous parking from a taz centers file, such as test/input/beamville/taz-centers.csv
    * @param geoObjects geo objects that should be used to hold parking stalls
    * @param parkingTypes the parking types we are generating, by default, the complete set
    * @return the parking accumulator
    */
  def generateDefaultParkingAccumulatorFromGeoObjects(
    geoObjects: Iterable[TAZ],
    random: Random,
    defaultReservedFor: ReservedFor,
    parkingTypes: Seq[ParkingType] = ParkingType.AllTypes,
    parkingLoadingAcc: ParkingLoadingAccumulator = ParkingLoadingAccumulator()
  ): ParkingLoadingAccumulator = {
    val result =
      generateDefaultParking(geoObjects, random, defaultReservedFor, parkingTypes, parkingLoadingAcc)
    logger.info(
      s"generated ${result.totalRows} parking zones,one for each provided geo level, with ${result.parkingStallsPlainEnglish} stalls (${result.totalParkingStalls}) in system"
    )
    if (result.someRowsFailed) {
      logger.warn(s"${result.failedRows} rows of parking data failed to load")
    }
    result
  }

  /**
    * generates ubiquitous parking from the contents of a TAZ centers file
    * @param geoObjects an iterable of geo objects
    * @param parkingTypes the parking types we are generating, by default, the complete set
    * @return parking zones and parking search tree
    */
  private def generateDefaultParking(
    geoObjects: Iterable[TAZ],
    random: Random,
    defaultReservedFor: ReservedFor,
    parkingTypes: Seq[ParkingType] = ParkingType.AllTypes,
    parkingLoadingAcc: ParkingLoadingAccumulator = ParkingLoadingAccumulator()
  ): ParkingLoadingAccumulator = {

    val rows: Iterable[String] = for {
      geoObj      <- geoObjects
      parkingType <- parkingTypes
      // We have to pass parking types: Some(CustomChargingPoint) and None
      // None is `NoCharger` which will allow non-charger ParkingZones. Check `returnSpotsWithoutChargers` in `ZonalParkingManager`
      maybeChargingPoint <- Seq(Some(ChargingPointType.CustomChargingPoint("DCFast", "50", "DC")), None) // NoCharger
    } yield {
      defaultParkingRow(geoObj.tazId, parkingType, maybeChargingPoint, defaultReservedFor)
    }

    val withHeader = Iterator.single(ParkingFileHeader) ++ rows
    fromIterator(withHeader, None, None, random, Some(defaultReservedFor), parkingLoadingAcc = parkingLoadingAcc)
  }

  /**
    * Write parking zones to csv.
    */
  @SuppressWarnings(Array("UnusedMethodParameter")) // TODO: scapegoat bug?
  def toCsv(parkingZones: Map[Id[ParkingZoneId], ParkingZone], filePath: String): Unit = {
    val df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
    df.setMaximumFractionDigits(7)
    val fileContent = parkingZones.values.toIndexedSeq
      .sortBy(_.parkingZoneId)
      .map { parkingZone =>
        val linkCoordMaybe = parkingZone.link.map(_.getCoord)
        List(
          parkingZone.tazId,
          parkingZone.parkingType,
          parkingZone.pricingModel.getOrElse(""),
          parkingZone.chargingPointType.getOrElse(""),
          parkingZone.maxStalls,
          parkingZone.pricingModel.map(pricing => df.format(pricing.costInDollars * 100)).getOrElse(""),
          VehicleManager.reserveForToString(parkingZone.reservedFor),
          parkingZone.timeRestrictions.map(x => x._1.toString + "|" + x._2.toString).mkString(";"),
          parkingZone.parkingZoneId,
          linkCoordMaybe.map(_.getX.toString).getOrElse(""),
          linkCoordMaybe.map(_.getY.toString).getOrElse(""),
          parkingZone.sitePowerManager.getOrElse(""),
          parkingZone.energyStorageCapacityInKWh.map(_.toString).getOrElse(""),
          parkingZone.energyStorageSOC.map(_.toString).getOrElse("")
        ).mkString(",")
      }
      .mkString(System.lineSeparator())

    FileUtils.writeToFile(filePath, Some(ParkingFileHeader), fileContent, None)
  }

  def rideHailParkingOutputDataDescriptor: OutputDataDescriptor =
    OutputDataDescriptorObject("ParkingZoneFileUtils", s"ridehailParking.csv")(
      """
      taz                         | Taz id where the parking zone resides                             
      parkingType                 | Parking type: Residential, Workplace, Public                                      
      pricingModel                | Pricing model                                        
      chargingPointType           | Charging point type                                           
      numStalls                   | Number of stalls                                   
      feeInCents                  | Fee in cents                                     
      reservedFor                 | Id of Vehicle Manager this zone is reserver for                                     
      timeRestrictions            | Time restrictions for vehicle categories                                           
      parkingZoneId               | Parking zone id                                       
      locationX                   | X part of a concrete location of this parking zone (if defined)                                   
      locationY                   | Y part of a concrete location of this parking zone (if defined)                                   
      sitePowerManager            | Site power manager
      energyStorageCapacityInKWh  | Energy storage capacity in KWh
      energyStorageSOC            | Energy storage state of charge
          """
    )

}
