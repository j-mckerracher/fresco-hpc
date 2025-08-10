"use client";
import * as vg from "@uwdata/vgplot";
import React from "react";
import { useCallback, useEffect, useRef } from "react";
import { AsyncDuckDB } from "duckdb-wasm-kit";
import { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

interface VGMenuProps {
  db: AsyncDuckDB;
  conn: AsyncDuckDBConnection;
  crossFilter: unknown;
  dbLoading: boolean;
  dataLoading: boolean;
  tableName: string;
  columnName: string;
  width: number;
  label: string;
}

const VgPlot: React.FC<VGMenuProps> = ({
  db,
  conn,
  crossFilter,
  dbLoading,
  dataLoading,
  tableName,
  columnName,
  label,
}) => {
  const plotsRef = useRef<HTMLDivElement | null>(null);
  const setupDb = useCallback(async () => {
    if (!dbLoading && db && !dataLoading) {
      //@ts-expect-error idk
      vg.coordinator().databaseConnector(
        vg.wasmConnector({
          duckdb: db,
          connection: conn,
        })
      );

      const plot = vg.menu({
        filterBy: crossFilter,
        as: crossFilter,
        column: columnName,
        from: tableName,
        label: " ",
      });
      //@ts-expect-error just work
      plotsRef.current?.replaceChildren(plot);
    }
  }, [dbLoading, db, dataLoading, conn, crossFilter, columnName, tableName]);

  useEffect(() => {
    setupDb();
  }, [setupDb]);

  return (
    <div className="flex flex-col w-full">
      <h1 className="text-white">{label}</h1>
      <div className="overflow-visible w-full" ref={plotsRef} />
    </div>
  );
};

export default React.memo(VgPlot);
