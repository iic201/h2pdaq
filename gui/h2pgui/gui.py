from __future__ import annotations

import argparse
import csv
import queue
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from itertools import cycle
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

import grpc
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from .grpc.gui_grpc import (
    GuiServiceClient,
    PreviewPoint,
    ServiceStatus,
    frame_to_points,
    get_services,
)
from .gui_ui import build_main_window_ui

pg.setConfigOptions(antialias=True)

STREAM_QUEUE_MAX_ITEMS = 500
STREAM_DRAIN_MAX_ITEMS = 75
STREAM_DRAIN_BUDGET_SECONDS = 0.015
TICK_INTERVAL_MS = 100
RENDER_INTERVAL_SECONDS = 0.25
MAX_SERIES_SAMPLES = 1200
MAX_FRAME_HISTORY = 1000
MAX_PLOT_SAMPLES_PER_SERIES = 400
MAX_TEXT_SERIES = 32


@dataclass(slots=True)
class Series:
    values: deque[tuple[float, float]]
    unit: str = ""

class InstrumentPreviewWindow(QtWidgets.QMainWindow):
    series_palette = ["#2563eb", "#dc2626", "#059669", "#7c3aed", "#ea580c", "#0891b2"]

    def __init__(
        self,
        *,
        initial_target: str = "",
        initial_source: str = "",
        manager_addr: str = "127.0.0.1:50051",
        interval_seconds: float = 0.1,
        history_seconds: float = 60.0,
    ) -> None:
        super().__init__()
        self.interval_seconds = interval_seconds
        self.history_seconds = history_seconds
        self.client: GuiServiceClient | None = None
        self.target = initial_target
        self.source = initial_source
        self.manager_addr = manager_addr
        self.stream_q: queue.Queue[tuple[Any, list[PreviewPoint]]] = queue.Queue(
            maxsize=STREAM_QUEUE_MAX_ITEMS
        )
        self.services_q: queue.Queue[list[ServiceStatus]] = queue.Queue()
        self.errors_q: queue.Queue[str] = queue.Queue()
        self.stop_event = threading.Event()
        self.stream_paused_event = threading.Event()
        self._active_stream_call: Any | None = None
        self._stream_call_lock = threading.Lock()
        self.series: dict[str, Series] = defaultdict(
            lambda: Series(deque(maxlen=MAX_SERIES_SAMPLES))
        )
        self.curves: dict[str, pg.PlotDataItem] = {}
        self.series_colors: dict[str, str] = {}
        self._color_cycle = cycle(self.series_palette)
        self.frame_history: deque[tuple[float, Any]] = deque(maxlen=MAX_FRAME_HISTORY)
        self.latest_timestamp: float | None = None
        self.selection_region: pg.LinearRegionItem | None = None
        self.selection_label: pg.TextItem | None = None
        self.latest_frame: Any | None = None
        self.services_by_address: dict[str, ServiceStatus] = {}
        self._last_error = ""
        self._discovery_error = ""
        self._latest_status_point: PreviewPoint | None = None
        self._dropped_stream_items = 0
        self._last_render_monotonic = 0.0

        self.setWindowTitle("h2pgui")
        self.resize(1080, 640)
        self.setMinimumSize(760, 440)
        self._build_ui()
        self._start_stream()
        self._start_service_refresh()

        self._tick_timer = QtCore.QTimer(self)
        self._tick_timer.setInterval(TICK_INTERVAL_MS)
        self._tick_timer.timeout.connect(self._tick)
        self._tick_timer.start()

        if self.target:
            self._switch_target(self.target, source=self.source)

    # Build the UI using the gui_ui helper module
    def _build_ui(self) -> None:
        build_main_window_ui(self)

    def _set_status(self, message: str) -> None:
        self.statusBar().showMessage(message)

    def _set_discovery_error(self, message: str) -> None:
        self._discovery_error = message
        self.services_count_label.setText("!")
        self.services_count_label.setToolTip(message)
        self.services_count_label.setStyleSheet("color: #b91c1c; font-weight: 700;")
        if self.client is None:
            self.context_label.setText("Discovery unavailable. Enter a target address or start the current manager.")

    def _clear_discovery_error(self) -> None:
        self._discovery_error = ""
        self.services_count_label.setToolTip("")
        self.services_count_label.setStyleSheet("color: #6b7280;")

    def _selection_window_seconds(self) -> float:
        return max(1.0, float(self.window_spin.value()))

    def _selection_bounds(self) -> tuple[float, float] | None:
        if self.selection_region is None:
            return None
        start, end = self.selection_region.getRegion()
        if end <= start:
            return None
        return float(start), float(end)

    def _frame_timestamp(self, frame: Any) -> float:
        observed_at = frame.observed_at.ToDatetime()
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        return observed_at.timestamp()

    def _trim_series_history(self, latest_timestamp: float | None = None) -> None:
        current_timestamp = latest_timestamp if latest_timestamp is not None else self.latest_timestamp
        if current_timestamp is None:
            current_timestamp = time.time()
        cutoff = current_timestamp - max(self._selection_window_seconds(), self.history_seconds)
        for series in self.series.values():
            while series.values and series.values[0][0] < cutoff:
                series.values.popleft()

    def _trim_frame_history(self, latest_timestamp: float | None = None) -> None:
        current_timestamp = latest_timestamp if latest_timestamp is not None else self.latest_timestamp
        if current_timestamp is None:
            current_timestamp = time.time()
        cutoff = current_timestamp - max(self._selection_window_seconds(), self.history_seconds)
        while self.frame_history and self.frame_history[0][0] < cutoff:
            self.frame_history.popleft()

    def _selection_window_changed(self, value: float) -> None:
        self.history_seconds = max(1.0, float(value))
        self._trim_series_history()
        self._trim_frame_history()
        self._reset_selection_to_latest()
        self._refresh_plot()

    def _selection_region_changed(self) -> None:
        if self.selection_region is None:
            return
        bounds = self._selection_bounds()
        if bounds is None:
            return
        start, end = bounds
        if self.selection_label is not None:
            self.selection_label.setText(
                f"select {end - start:.1f}s from {datetime.fromtimestamp(start, tz=timezone.utc).strftime('%H:%M:%S')} to {datetime.fromtimestamp(end, tz=timezone.utc).strftime('%H:%M:%S')}"
            )
            y_range = self.plot_widget.viewRange()[1]
            self.selection_label.setPos(start, y_range[1])

    def _reset_selection_to_latest(self) -> None:
        if self.selection_region is None:
            return
        latest_timestamp = self.latest_timestamp
        if latest_timestamp is None:
            return
        start = latest_timestamp - self._selection_window_seconds()
        end = latest_timestamp
        self.selection_region.blockSignals(True)
        try:
            self.selection_region.setRegion((start, end))
        finally:
            self.selection_region.blockSignals(False)
        self._selection_region_changed()

    def _clear_display_data(self, checked: bool = False, *, announce: bool = True) -> None:
        _ = checked
        self.stream_q = queue.Queue(maxsize=STREAM_QUEUE_MAX_ITEMS)
        self.series.clear()
        self.curves.clear()
        self.series_colors.clear()
        self._color_cycle = cycle(self.series_palette)
        self.frame_history.clear()
        self.latest_timestamp = None
        self.latest_frame = None
        self._latest_status_point = None
        self._dropped_stream_items = 0
        self._last_render_monotonic = 0.0
        self.latest_text.clear()
        self.plot_widget.clear()
        self.plot_widget.addLegend(offset=(12, 12))
        if self.selection_region is not None:
            self.selection_region.setParentItem(self.plot_widget.getPlotItem())
            self.plot_widget.addItem(self.selection_region)
        if self.selection_label is not None:
            self.selection_label.setText("")
            self.selection_label.setParentItem(self.plot_widget.getPlotItem())
            self.plot_widget.addItem(self.selection_label, ignoreBounds=True)
        self._refresh_plot()
        if announce:
            self._set_status("GUI data cleared")

    def _record_frame(self, frame: Any) -> None:
        timestamp = self._frame_timestamp(frame)
        self.latest_frame = frame
        self.latest_timestamp = timestamp
        self.frame_history.append((timestamp, frame))
        if self.selection_region is not None:
            current_bounds = self._selection_bounds()
            if current_bounds is None:
                self._reset_selection_to_latest()

    def _load_info(self) -> None:
        if self.client is None:
            self.title_label.setText("h2pgui")
            self.context_label.setText("Select a server from the list and connect.")
            self._set_sources(())
            self._set_status("Select a server from the list")
            return
        try:
            info = self.client.get_info()
        except Exception as exc:
            self._set_status(f"Info unavailable: {exc}")
            return

        source = self.source or (info.sources[0] if info.sources else "")
        self.source = source
        self._set_sources(info.sources, source)
        label = info.display_name or info.instrument_id or self.target
        self.title_label.setText(label)
        self.context_label.setText(f"{info.service_name} at {self.target} source={source or '*'}")
        self._last_error = ""
        self._set_status(f"Connected to {self.target}")

    def _set_sources(self, sources: tuple[str, ...], selected: str = "") -> None:
        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        choices = list(sources) or [""]
        if selected and selected not in choices:
            choices.append(selected)
        for source in choices:
            self.source_combo.addItem(source or "*", source)
        current = selected if selected in choices else choices[0]
        self.source_combo.setCurrentIndex(max(0, choices.index(current)))
        self.source_combo.setEnabled(bool(sources))
        self.source_combo.blockSignals(False)

    def _source_selected(self, index: int) -> None:
        source = self.source_combo.itemData(index) or ""
        if source == self.source:
            return
        self._switch_target(self.target, source=source)

    def _cancel_active_stream(self) -> None:
        with self._stream_call_lock:
            stream_call = self._active_stream_call
        if stream_call is not None:
            try:
                stream_call.cancel()
            except Exception:
                pass

    def _start_stream(self) -> None:
        thread = threading.Thread(target=self._stream_worker, daemon=True)
        thread.start()

    def _start_service_refresh(self) -> None:
        thread = threading.Thread(target=self._service_worker, daemon=True)
        thread.start()

    def _refresh_services(self) -> None:
        thread = threading.Thread(target=self._service_once, daemon=True)
        thread.start()

    def _service_once(self) -> None:
        try:
            self.services_q.put(get_services(self.manager_addr))
        except Exception as exc:
            self.errors_q.put(f"Discovery: {exc}")

    def _service_worker(self) -> None:
        while not self.stop_event.is_set():
            self._service_once()
            for _ in range(50):
                if self.stop_event.is_set():
                    return
                time.sleep(0.1)

    def _put_stream_item(self, frame: Any, points: list[PreviewPoint]) -> None:
        item = (frame, points)
        while not self.stop_event.is_set():
            try:
                self.stream_q.put_nowait(item)
                return
            except queue.Full:
                try:
                    self.stream_q.get_nowait()
                    self._dropped_stream_items += 1
                except queue.Empty:
                    continue

    def _stream_worker(self) -> None:
        while not self.stop_event.is_set():
            if self.stream_paused_event.is_set():
                time.sleep(0.1)
                continue
            client = self.client
            if client is None:
                time.sleep(0.2)
                continue
            stream_call = None
            try:
                stream_call = client.open_frame_stream(
                    source=self.source,
                    interval_seconds=self.interval_seconds,
                    emit_on_change_only=True,
                )
                with self._stream_call_lock:
                    self._active_stream_call = stream_call
                for response in stream_call:
                    if self.stop_event.is_set():
                        return
                    if self.stream_paused_event.is_set():
                        break
                    frame = response.frame
                    try:
                        points = frame_to_points(frame)
                    except Exception as exc:
                        points = []
                        self.errors_q.put(f"Frame parse error: {exc}")
                    self._put_stream_item(frame, points)
            except grpc.RpcError as exc:
                if self.stop_event.is_set():
                    return
                if exc.code() == grpc.StatusCode.CANCELLED:
                    continue
                self.errors_q.put(f"Stream error: {exc.code().name} {exc.details()}")
                time.sleep(1.0)
            except Exception as exc:
                self.errors_q.put(f"Stream error: {exc}")
                time.sleep(1.0)
            finally:
                if stream_call is not None:
                    with self._stream_call_lock:
                        if self._active_stream_call is stream_call:
                            self._active_stream_call = None

    def _tick(self) -> None:
        changed = False
        processed_stream_items = 0
        stream_deadline = time.monotonic() + STREAM_DRAIN_BUDGET_SECONDS

        while processed_stream_items < STREAM_DRAIN_MAX_ITEMS:
            if processed_stream_items and time.monotonic() >= stream_deadline:
                break
            try:
                frame, points = self.stream_q.get_nowait()
            except queue.Empty:
                break
            self._record_frame(frame)
            for point in points:
                self._record_point(point)
            processed_stream_items += 1
            changed = True

        if changed and self.latest_timestamp is not None:
            self._trim_series_history(self.latest_timestamp)
            self._trim_frame_history(self.latest_timestamp)

        while True:
            try:
                message = self.errors_q.get_nowait()
            except queue.Empty:
                break
            if message.startswith("Discovery: "):
                self._set_discovery_error(message.removeprefix("Discovery: "))
                continue
            if message != self._last_error:
                self._last_error = message
                self._set_status(message)

        while True:
            try:
                services = self.services_q.get_nowait()
            except queue.Empty:
                break
            self._render_services(services)

        if changed:
            if self._dropped_stream_items:
                self._set_status(f"Preview skipped {self._dropped_stream_items} stale frames to keep up")
                self._dropped_stream_items = 0
            elif self._latest_status_point is not None:
                point = self._latest_status_point
                self._set_status(
                    f"seq={point.sequence_id} {point.name}={point.value:g}{(' ' + point.unit) if point.unit else ''}"
                )
            now = time.monotonic()
            if now - self._last_render_monotonic >= RENDER_INTERVAL_SECONDS:
                self._last_render_monotonic = now
                self._update_latest_text()
                self._refresh_plot()

    def _render_services(self, services: list[ServiceStatus]) -> None:
        selected = self._selected_service_address()
        self.services_by_address = {service.address: service for service in services}
        self.service_model.set_services(services)
        self._clear_discovery_error()
        self.services_count_label.setText(str(len(services)))

        selected = selected or self.target
        if selected:
            selection_model = self.service_view.selectionModel()
            if selection_model is not None:
                for row, service in enumerate(services):
                    if service.address == selected:
                        index = self.service_model.index(row, 0)
                        selection_model.select(
                            index,
                            QtCore.QItemSelectionModel.SelectionFlag.ClearAndSelect
                            | QtCore.QItemSelectionModel.SelectionFlag.Rows,
                        )
                        break

    def _selected_service_address(self) -> str:
        model = self.service_view.selectionModel()
        if model is None:
            return ""
        rows = model.selectedRows()
        if not rows:
            return ""
        service = self.service_model.service_at(rows[0].row())
        return service.address if service is not None else ""

    def _on_service_select(self) -> None:
        selected = self._selected_service_address()
        if not selected:
            return
        service = self.services_by_address.get(selected)
        if service is None:
            return
        self.target_edit.setText(service.address)
        if service.gui_connectable:
            self._set_status(f"Selected {service.name} at {service.address}")
        else:
            self._set_status(f"Selected {service.name} but GuiService is unavailable")

    def _connect_selected_service(self) -> None:
        typed_address = self.target_edit.text().strip()
        if typed_address:
            self._switch_target(typed_address)
            return

        address = self._selected_service_address()
        if not address:
            self._set_status("Select a server or enter a target address")
            return
        service = self.services_by_address.get(address)
        if service is None:
            self._set_status("Selected server is no longer available")
            return
        if not service.gui_connectable:
            self._set_status(f"{service.name} cannot be reached via GuiService")
            return
        self._switch_target(service.address)

    def _switch_target(self, target: str, *, source: str = "") -> None:
        if not target:
            self._set_status("Select a server first")
            return
        self.target_edit.setText(target)
        self._cancel_active_stream()
        if self.client is not None:
            self.client.close()
        self.client = GuiServiceClient(target)
        self.target = target
        self.source = source
        self._clear_display_data(announce=False)
        self._load_info()

    def _record_point(self, point: PreviewPoint) -> None:
        timestamp = point.timestamp.timestamp()
        series = self.series[point.name]
        series.values.append((timestamp, point.value))
        series.unit = point.unit or series.unit
        self.latest_timestamp = timestamp
        self._latest_status_point = point

    def _save_history_interval(self) -> None:
        if self.client is None:
            self._set_status("Select and connect to a server first")
            return
        if not self.frame_history:
            self._set_status("No frames available to save yet")
            return

        bounds = self._selection_bounds()
        if bounds is None:
            self._set_status("Drag a time interval on the plot first")
            return

        try:
            response = self._save_interval(bounds)
            if response.saved_count:
                self._set_status(
                    f"Saved {response.saved_count} frames sequence={response.first_sequence_id}-{response.last_sequence_id}"
                )
            else:
                self._set_status("No frames were saved for the selected interval")
        except Exception as exc:
            self._set_status(f"Save failed: {exc}")

    def _save_interval(self, bounds: tuple[float, float]):
        if self.client is None:
            raise RuntimeError("Not connected")

        start, end = bounds
        if end <= start:
            raise RuntimeError("Invalid selection interval")

        frames = [frame for timestamp, frame in self.frame_history if start <= timestamp <= end]
        if not frames:
            raise RuntimeError("No frames in the selected interval")

        sequence_ids = [int(frame.sequence_id) for frame in frames]
        integrals = self._selection_integrals(bounds)
        return self.client.save_interval(
            source=self.source,
            start_observed_at=datetime.fromtimestamp(start, tz=timezone.utc),
            end_observed_at=datetime.fromtimestamp(end, tz=timezone.utc),
            start_sequence_id=min(sequence_ids),
            end_sequence_id=max(sequence_ids),
            analysis=self._integral_analysis(bounds, integrals),
        )

    def _save_selection_locally(self) -> None:
        bounds = self._selection_bounds()
        if bounds is None:
            self._set_status("Drag a time interval on the plot first")
            return

        try:
            export_dir, frame_count, point_count = self._export_selection_locally(bounds)
            self._set_status(
                f"Saved local selection: {frame_count} frames, {point_count} points to {export_dir}"
            )
        except Exception as exc:
            self._set_status(f"Local save failed: {exc}")

    def _export_selection_locally(self, bounds: tuple[float, float]) -> tuple[Path, int, int]:
        start, end = bounds
        if end <= start:
            raise RuntimeError("Invalid selection interval")

        points = self._selected_points(bounds)
        integrals = self._selection_integrals(bounds)
        if not points and not integrals:
            raise RuntimeError("No data in the selected interval")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        export_dir = (
            Path("data")
            / "gui_exports"
            / self._safe_filename_part(self.target or "local")
            / f"{self._safe_filename_part(self.source or 'all')}_{stamp}_{int(start)}_{int(end)}"
        )
        export_dir.mkdir(parents=True, exist_ok=True)

        points_path = export_dir / "points.csv"
        with points_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["series", "timestamp", "datetime_utc", "value", "unit"],
            )
            writer.writeheader()
            for name, timestamp, value, unit in points:
                writer.writerow(
                    {
                        "series": name,
                        "timestamp": f"{timestamp:.9f}",
                        "datetime_utc": datetime.fromtimestamp(
                            timestamp,
                            tz=timezone.utc,
                        ).isoformat(),
                        "value": value,
                        "unit": unit,
                    }
                )

        integrals_path = export_dir / "integrals.csv"
        with integrals_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "series",
                    "integral",
                    "integral_unit",
                    "value_unit",
                    "time_unit",
                    "method",
                    "selected_start_utc",
                    "selected_end_utc",
                    "integrated_start_utc",
                    "integrated_end_utc",
                    "duration_seconds",
                    "sample_count",
                ],
            )
            writer.writeheader()
            for item in integrals:
                writer.writerow(item)

        return export_dir, 0, len(points)

    def _selected_frames(self, bounds: tuple[float, float]) -> list[tuple[float, Any]]:
        start, end = bounds
        return [
            (timestamp, frame)
            for timestamp, frame in self.frame_history
            if start <= timestamp <= end
        ]

    def _selected_points(self, bounds: tuple[float, float]) -> list[tuple[str, float, float, str]]:
        start, end = bounds
        points = []
        for name, series in sorted(self.series.items()):
            for timestamp, value in series.values:
                if start <= timestamp <= end:
                    points.append((name, timestamp, value, series.unit))
        return points

    def _safe_filename_part(self, value: str) -> str:
        cleaned = [
            character if character.isalnum() or character in ("-", "_", ".") else "_"
            for character in value
        ]
        return "".join(cleaned).strip("._") or "unknown"

    def _calculate_selection_integral(self) -> None:
        bounds = self._selection_bounds()
        if bounds is None:
            self._set_status("Drag a time interval on the plot first")
            return

        integrals = self._selection_integrals(bounds)
        if not integrals:
            self._set_status("No plottable samples in the selected interval")
            return

        self._set_status(f"Time integral calculated for {len(integrals)} series")
        QtWidgets.QMessageBox.information(
            self,
            "Time Integral",
            self._integral_message(bounds, integrals),
        )

    def _selection_integrals(self, bounds: tuple[float, float]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for name, series in sorted(self.series.items()):
            result = self._integral_for_series(list(series.values), bounds)
            if result is None:
                continue
            area, start, end, sample_count = result
            value_unit = series.unit
            results.append(
                {
                    "series": name,
                    "integral": area,
                    "integral_unit": f"{value_unit}*s" if value_unit else "value*s",
                    "value_unit": value_unit,
                    "time_unit": "s",
                    "method": "trapezoidal_time_integral",
                    "selected_start_utc": datetime.fromtimestamp(bounds[0], tz=timezone.utc).isoformat(),
                    "selected_end_utc": datetime.fromtimestamp(bounds[1], tz=timezone.utc).isoformat(),
                    "integrated_start_utc": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                    "integrated_end_utc": datetime.fromtimestamp(end, tz=timezone.utc).isoformat(),
                    "duration_seconds": end - start,
                    "sample_count": sample_count,
                }
            )
        return results

    def _integral_analysis(
        self,
        bounds: tuple[float, float],
        integrals: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "integration": {
                "method": "trapezoidal_time_integral",
                "description": "Integral of each plotted value with respect to time.",
                "time_unit": "s",
                "selected_start_utc": datetime.fromtimestamp(bounds[0], tz=timezone.utc).isoformat(),
                "selected_end_utc": datetime.fromtimestamp(bounds[1], tz=timezone.utc).isoformat(),
                "selected_duration_seconds": bounds[1] - bounds[0],
                "series": integrals,
            }
        }

    def _integral_message(
        self,
        bounds: tuple[float, float],
        integrals: list[dict[str, Any]],
    ) -> str:
        selected_start = datetime.fromtimestamp(bounds[0], tz=timezone.utc)
        selected_end = datetime.fromtimestamp(bounds[1], tz=timezone.utc)
        lines = [
            "Integral over time",
            "Method: trapezoidal rule applied to value vs. timestamp.",
            f"Selection: {selected_start.isoformat()} to {selected_end.isoformat()}",
            f"Selection duration: {bounds[1] - bounds[0]:.6g} s",
            "",
        ]
        for item in integrals:
            lines.extend(
                [
                    str(item["series"]),
                    f"  integral: {item['integral']:.12g} {item['integral_unit']}",
                    f"  data span: {item['duration_seconds']:.6g} s using {item['sample_count']} samples",
                ]
            )
        return "\n".join(lines)

    def _integral_for_series(
        self,
        values: list[tuple[float, float]],
        bounds: tuple[float, float],
    ) -> tuple[float, float, float, int] | None:
        if len(values) < 2:
            return None

        start, end = bounds
        if end <= start:
            return None

        values = sorted(values)
        points = [(timestamp, value) for timestamp, value in values if start <= timestamp <= end]

        start_value = self._interpolated_value(values, start)
        if start_value is not None:
            points.insert(0, (start, start_value))

        end_value = self._interpolated_value(values, end)
        if end_value is not None:
            points.append((end, end_value))

        deduped: list[tuple[float, float]] = []
        for timestamp, value in sorted(points):
            if deduped and timestamp == deduped[-1][0]:
                deduped[-1] = (timestamp, value)
            else:
                deduped.append((timestamp, value))

        if len(deduped) < 2:
            return None

        area = 0.0
        for (left_t, left_v), (right_t, right_v) in zip(deduped, deduped[1:]):
            area += (right_t - left_t) * (left_v + right_v) / 2.0
        return area, deduped[0][0], deduped[-1][0], len(deduped)

    def _interpolated_value(
        self,
        values: list[tuple[float, float]],
        timestamp: float,
    ) -> float | None:
        if not values or timestamp < values[0][0] or timestamp > values[-1][0]:
            return None

        for index, (current_t, current_v) in enumerate(values):
            if current_t == timestamp:
                return current_v
            if current_t > timestamp and index > 0:
                previous_t, previous_v = values[index - 1]
                span = current_t - previous_t
                if span <= 0.0:
                    return current_v
                fraction = (timestamp - previous_t) / span
                return previous_v + fraction * (current_v - previous_v)
        return None

    def _series_color(self, name: str) -> str:
        color = self.series_colors.get(name)
        if color is None:
            color = next(self._color_cycle)
            self.series_colors[name] = color
        return color

    def _plot_samples(
        self,
        values: list[tuple[float, float]],
        latest_timestamp: float,
    ) -> list[tuple[float, float]]:
        visible_start = latest_timestamp - self.history_seconds
        visible_values = [
            (timestamp, value)
            for timestamp, value in values
            if timestamp >= visible_start
        ]
        if len(visible_values) <= MAX_PLOT_SAMPLES_PER_SERIES:
            return visible_values

        stride = max(
            1,
            (len(visible_values) + MAX_PLOT_SAMPLES_PER_SERIES - 1)
            // MAX_PLOT_SAMPLES_PER_SERIES,
        )
        samples = visible_values[::stride]
        if samples[-1] != visible_values[-1]:
            samples.append(visible_values[-1])
        return samples

    def _refresh_plot(self) -> None:
        latest_timestamp = self.latest_timestamp
        min_t = None
        max_t = None
        min_v = None
        max_v = None

        if latest_timestamp is None:
            self.plot_widget.setTitle("Waiting for frames")
            return

        plotted_names: set[str] = set()
        for name, series in sorted(self.series.items()):
            values = self._plot_samples(list(series.values), latest_timestamp)
            if not values:
                continue
            plotted_names.add(name)
            timestamps = [timestamp for timestamp, _ in values]
            samples = [value for _, value in values]
            curve = self.curves.get(name)
            if curve is None:
                curve = self.plot_widget.plot(
                    timestamps,
                    samples,
                    pen=pg.mkPen(color=self._series_color(name), width=2),
                )
                self.curves[name] = curve

            label = f"{name} [{series.unit}]" if series.unit else name
            curve.setData(timestamps, samples, name=label)

            current_min_t = timestamps[0]
            current_max_t = timestamps[-1]
            current_min_v = min(samples)
            current_max_v = max(samples)
            min_t = current_min_t if min_t is None else min(min_t, current_min_t)
            max_t = current_max_t if max_t is None else max(max_t, current_max_t)
            min_v = current_min_v if min_v is None else min(min_v, current_min_v)
            max_v = current_max_v if max_v is None else max(max_v, current_max_v)

        for name in list(self.curves):
            if name not in plotted_names:
                self.plot_widget.removeItem(self.curves.pop(name))

        if min_t is None or max_t is None or min_v is None or max_v is None:
            self.plot_widget.setTitle("Waiting for frames")
            return

        self.plot_widget.setTitle("")
        x_start = max(min_t, latest_timestamp - self.history_seconds)
        x_end = max(max_t, x_start + 1.0)
        if x_end <= x_start:
            x_end = x_start + 1.0

        if max_v <= min_v:
            pad = max(abs(max_v), 1.0) * 0.1
            min_v -= pad
            max_v += pad

        self.plot_widget.setXRange(x_start, x_end, padding=0.01)
        self.plot_widget.setYRange(min_v, max_v, padding=0.15)
        self._selection_region_changed()

    def _update_latest_text(self) -> None:
        if self.latest_frame is None:
            self.latest_text.clear()
            return
        frame = self.latest_frame
        payload_kind = frame.payload.WhichOneof("payload")
        lines = [
            f"source: {frame.source}",
            f"producer: {frame.producer_id}",
            f"sequence: {frame.sequence_id}",
            f"kind: {frame.kind}",
            f"payload: {payload_kind or 'empty'}",
            "",
        ]
        has_values = False
        for name, series in sorted(self.series.items())[:MAX_TEXT_SERIES]:
            if series.values:
                has_values = True
                lines.append(f"{name}: {series.values[-1][1]:g} {series.unit}".rstrip())
        if not has_values:
            lines.append("No numeric values in this frame")
        self.latest_text.setPlainText("\n".join(lines))

    def _start_local_stream(self) -> None:
        if self.client is None:
            self._set_status("Select and connect to a server first")
            return
        if not self.stream_paused_event.is_set():
            self._set_status("Preview stream is already running")
            return
        self.stream_paused_event.clear()
        self._set_status("Preview stream started")

    def _stop_local_stream(self) -> None:
        if self.client is None:
            self._set_status("Select and connect to a server first")
            return
        if self.stream_paused_event.is_set():
            self._set_status("Preview stream is already stopped")
            return
        self.stream_paused_event.set()
        self._cancel_active_stream()
        self._set_status("Preview stream stopped")

    def close(self) -> None:
        self.stop_event.set()
        self._cancel_active_stream()
        if self.client is not None:
            self.client.close()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:  # noqa: N802
        self.close()
        event.accept()


def main() -> None:
    parser = argparse.ArgumentParser(description="h2pcontrol instrument preview GUI")
    parser.add_argument("--target", default="", help="GuiService address to connect to on startup")
    parser.add_argument(
        "--manager-addr",
        default="127.0.0.1:50051",
        help="Manager service address used for server discovery",
    )
    parser.add_argument("--source", default="", help="Initial source to stream from")
    parser.add_argument("--interval-seconds", type=float, default=0.1)
    parser.add_argument("--history-seconds", type=float, default=60.0)
    args = parser.parse_args()

    application = QtWidgets.QApplication(sys.argv)
    application.setApplicationName("h2pgui")
    window = InstrumentPreviewWindow(
        initial_target=args.target,
        initial_source=args.source,
        manager_addr=args.manager_addr,
        interval_seconds=args.interval_seconds,
        history_seconds=args.history_seconds,
    )
    window.show()
    raise SystemExit(application.exec())


if __name__ == "__main__":
    main()
