import type { FlightRecord } from "./types";

/** Cylinder head temp caution (demo band). */
export const CHT_HIGH_F = 420;
/** Cylinder head temp dangerous (demo band). */
export const CHT_DANGEROUS_F = 460;
/** Oil temp caution (demo band). */
export const OIL_TEMP_HIGH_F = 215;
/** Oil temp dangerous (demo band). */
export const OIL_TEMP_DANGEROUS_F = 235;
/** EGT caution for piston cruise context (demo). */
export const EGT_HIGH_F = 1350;
/** EGT dangerous (demo band). */
export const EGT_DANGEROUS_F = 1450;
/** Oil pressure low caution (psi). */
export const OIL_PSI_MIN_LOW = 55;
/** Oil pressure low dangerous (psi). */
export const OIL_PSI_MIN_DANGEROUS = 45;
/** Oil pressure high caution (psi). */
export const OIL_PSI_MAX_HIGH = 85;
/** Oil pressure high dangerous (psi). */
export const OIL_PSI_MAX_DANGEROUS = 95;

export type TelemetrySeverity = "ok" | "warn" | "bad";

export function isChtHigh(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= CHT_HIGH_F;
}

export function isChtDangerous(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= CHT_DANGEROUS_F;
}

export function isOilTempHigh(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= OIL_TEMP_HIGH_F;
}

export function isOilTempDangerous(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= OIL_TEMP_DANGEROUS_F;
}

export function isEgtHigh(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= EGT_HIGH_F;
}

export function isEgtDangerous(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= EGT_DANGEROUS_F;
}

export function isOilPsiMinLow(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v <= OIL_PSI_MIN_LOW;
}

export function isOilPsiMinDangerous(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v <= OIL_PSI_MIN_DANGEROUS;
}

export function isOilPsiMaxHigh(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= OIL_PSI_MAX_HIGH;
}

export function isOilPsiMaxDangerous(v: number | null | undefined): boolean {
  return v !== null && v !== undefined && v >= OIL_PSI_MAX_DANGEROUS;
}

export type TelemetrySortField =
  | "cht_max"
  | "oil_temp_max"
  | "oil_pressure_min"
  | "oil_pressure_max"
  | "egt_max"
  | "fuel_used_gal";

export function telemetrySeverityForField(field: TelemetrySortField, rec: FlightRecord): TelemetrySeverity {
  if (field === "fuel_used_gal") return "ok";
  switch (field) {
    case "cht_max":
      return isChtDangerous(rec.cht_max) ? "bad" : isChtHigh(rec.cht_max) ? "warn" : "ok";
    case "oil_temp_max":
      return isOilTempDangerous(rec.oil_temp_max)
        ? "bad"
        : isOilTempHigh(rec.oil_temp_max)
          ? "warn"
          : "ok";
    case "oil_pressure_min":
      return isOilPsiMinDangerous(rec.oil_pressure_min)
        ? "bad"
        : isOilPsiMinLow(rec.oil_pressure_min)
          ? "warn"
          : "ok";
    case "oil_pressure_max":
      return isOilPsiMaxDangerous(rec.oil_pressure_max)
        ? "bad"
        : isOilPsiMaxHigh(rec.oil_pressure_max)
          ? "warn"
          : "ok";
    case "egt_max":
      return isEgtDangerous(rec.egt_max) ? "bad" : isEgtHigh(rec.egt_max) ? "warn" : "ok";
    default:
      return "ok";
  }
}

/** True if sort-preview / row styling should warn for this field and record (never fuel). */
export function telemetrySortFieldIsWarn(field: TelemetrySortField, rec: FlightRecord): boolean {
  return telemetrySeverityForField(field, rec) !== "ok";
}
