/**
 * Synthetic markets for the mock server. Tickers, titles, and numbers are invented to
 * resemble Kalshi's shapes (a one-cent grid, a tenth-of-a-cent grid, a tapered grid, and a
 * market whose metadata has not resolved); none is real data.
 */

import type { PriceRange } from "../../src/api/protocol.ts";

export interface MarketDefinition {
  readonly ticker: string;
  readonly eventTicker: string;
  readonly seriesTicker: string;
  readonly title: string | null;
  readonly subtitle: string | null;
  readonly category: string | null;
  readonly showcase: boolean;
  /** What the API reports; `null` models metadata that has not resolved. */
  readonly priceRanges: readonly PriceRange[] | null;
  /** The grid the simulator trades on, whatever the API reports. */
  readonly tradingRanges: readonly PriceRange[];
  readonly startMidE4: number;
  /** Price ticks the fair value moves per second, standard deviation. */
  readonly volatilityTicks: number;
  readonly tradesPerSecond: number;
  readonly churnPerSecond: number;
  readonly levelsPerSide: number;
  readonly spreadTicks: number;
  readonly medianLevelContracts: number;
  readonly volume24hContracts: number;
  readonly closesInHours: number;
}

const ONE_CENT: readonly PriceRange[] = [{ start_e4: 0, end_e4: 10_000, step_e4: 100 }];
const DECI_CENT: readonly PriceRange[] = [{ start_e4: 0, end_e4: 10_000, step_e4: 10 }];
const TAPERED: readonly PriceRange[] = [
  { start_e4: 0, end_e4: 1_000, step_e4: 10 },
  { start_e4: 1_000, end_e4: 9_000, step_e4: 100 },
  { start_e4: 9_000, end_e4: 10_000, step_e4: 10 },
];

export const MARKETS: readonly MarketDefinition[] = [
  {
    ticker: "MOCKBTC-26SEP1017-T64999.99",
    eventTicker: "MOCKBTC-26SEP1017",
    seriesTicker: "MOCKBTC",
    title: "Bitcoin price today at 5pm EDT?",
    subtitle: "$65,000 or above",
    category: "Crypto",
    showcase: true,
    priceRanges: DECI_CENT,
    tradingRanges: DECI_CENT,
    startMidE4: 5_430,
    volatilityTicks: 2.2,
    tradesPerSecond: 3.5,
    churnPerSecond: 45,
    levelsPerSide: 70,
    spreadTicks: 3,
    medianLevelContracts: 80,
    volume24hContracts: 1_840_000,
    closesInHours: 6,
  },
  {
    ticker: "MOCKHIGHNY-26SEP10-B84.5",
    eventTicker: "MOCKHIGHNY-26SEP10",
    seriesTicker: "MOCKHIGHNY",
    title: "Highest temperature in NYC today?",
    subtitle: "84° to 85°",
    category: "Climate and Weather",
    showcase: true,
    priceRanges: ONE_CENT,
    tradingRanges: ONE_CENT,
    startMidE4: 3_150,
    volatilityTicks: 0.35,
    tradesPerSecond: 1.4,
    churnPerSecond: 9,
    levelsPerSide: 30,
    spreadTicks: 1,
    medianLevelContracts: 180,
    volume24hContracts: 412_000,
    closesInHours: 11,
  },
  {
    ticker: "MOCKPAYROLLS-26OCT-T100000",
    eventTicker: "MOCKPAYROLLS-26OCT",
    seriesTicker: "MOCKPAYROLLS",
    title: "Jobs added in September?",
    subtitle: "Above 100,000",
    category: "Economics",
    showcase: false,
    priceRanges: TAPERED,
    tradingRanges: TAPERED,
    startMidE4: 1_180,
    volatilityTicks: 0.25,
    tradesPerSecond: 0.8,
    churnPerSecond: 12,
    levelsPerSide: 40,
    spreadTicks: 1,
    medianLevelContracts: 400,
    volume24hContracts: 265_000,
    closesInHours: 530,
  },
  {
    ticker: "MOCKUNRESOLVED-26SEP12-T3",
    eventTicker: "MOCKUNRESOLVED-26SEP12",
    seriesTicker: "MOCKUNRESOLVED",
    title: null,
    subtitle: null,
    category: null,
    showcase: false,
    priceRanges: null,
    tradingRanges: ONE_CENT,
    startMidE4: 7_600,
    volatilityTicks: 0.2,
    tradesPerSecond: 0.3,
    churnPerSecond: 4,
    levelsPerSide: 20,
    spreadTicks: 2,
    medianLevelContracts: 90,
    volume24hContracts: 38_000,
    closesInHours: 50,
  },
];
