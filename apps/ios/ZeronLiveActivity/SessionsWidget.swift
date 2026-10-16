import SwiftUI
import WidgetKit

/// sessions that need the user, then running ones with their elapsed time.
struct SessionsWidget: Widget {
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: WidgetStore.sessionsKind, provider: SnapshotProvider()) { entry in
            SessionsWidgetView(entry: entry)
                .zeronWidgetBackground()
        }
        .configurationDisplayName("Sessions")
        .description("Sessions waiting on you, and runs in progress with how long they've been going.")
        .supportedFamilies([
            .systemSmall, .systemMedium, .systemLarge,
            .accessoryCircular, .accessoryRectangular, .accessoryInline,
        ])
    }
}

private struct SessionsWidgetView: View {
    @Environment(\.widgetFamily) private var family
    let entry: SnapshotEntry

    var body: some View {
        if let snapshot = entry.snapshot {
            let asOf = entry.asOf

            switch family {
            case .systemSmall: SmallSessions(snapshot: snapshot, asOf: asOf)
            case .systemMedium: ListSessions(snapshot: snapshot, asOf: asOf, limit: 3)
            case .systemLarge: ListSessions(snapshot: snapshot, asOf: asOf, limit: 6)
            case .accessoryCircular: CircularSessions(snapshot: snapshot)
            case .accessoryRectangular: RectangularSessions(snapshot: snapshot, asOf: asOf)
            case .accessoryInline: Text(asOf.map { "\(snapshot.summary) · as of \($0.stamp)" } ?? snapshot.summary)
            default: ListSessions(snapshot: snapshot, asOf: asOf, limit: 3)
            }
        } else {
            SignedOut(compact: family.isAccessory)
        }
    }
}

private struct SmallSessions: View {
    let snapshot: WidgetSnapshot
    let asOf: Date?

    var body: some View {
        let top = snapshot.sessions.first

        VStack(alignment: .leading, spacing: 4) {
            Headline(snapshot: snapshot, asOf: asOf)

            Spacer(minLength: 6)

            if let top {
                HStack(spacing: 5) {
                    StatusIcon(status: top.status)
                    SessionTime(session: top, asOf: asOf)
                        .font(Face.mono(11))
                        .foregroundStyle(.secondary)
                }

                Text(top.title)
                    .font(Face.sans(13))
                    .lineLimit(2)
                    .privacySensitive()

                Text(top.detail)
                    .font(Face.sans(11))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .privacySensitive()
            } else {
                Text("No sessions running")
                    .font(Face.sans(13))
                    .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .widgetURL(top.map { ZeronLink.chat($0.id) } ?? ZeronLink.sessions)
    }
}

private struct ListSessions: View {
    let snapshot: WidgetSnapshot
    let asOf: Date?
    let limit: Int

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                Headline(snapshot: snapshot, asOf: asOf)
                Spacer()
                NewSessionButton()
            }

            if snapshot.sessions.isEmpty {
                Spacer()
                Text("No sessions running")
                    .font(Face.sans(13))
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity)
                Spacer()
            } else {
                ForEach(snapshot.sessions.prefix(limit)) { session in
                    Link(destination: ZeronLink.chat(session.id)) {
                        SessionLine(session: session, asOf: asOf)
                    }
                }
                Spacer(minLength: 0)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .widgetURL(ZeronLink.sessions)
    }
}

private struct CircularSessions: View {
    let snapshot: WidgetSnapshot

    var body: some View {
        let needsYou = snapshot.needsYou > 0

        ZStack {
            AccessoryWidgetBackground()

            VStack(spacing: 0) {
                Image(systemName: needsYou ? "hand.raised.fill" : "circle.dotted")
                    .font(.system(size: 12, weight: .semibold))
                Text("\(needsYou ? snapshot.needsYou : snapshot.working)")
                    .font(Face.mono(18))
            }
        }
        .widgetURL(snapshot.sessions.first.map { ZeronLink.chat($0.id) } ?? ZeronLink.sessions)
        .accessibilityLabel(snapshot.summary)
    }
}

private struct RectangularSessions: View {
    let snapshot: WidgetSnapshot
    let asOf: Date?

    var body: some View {
        let top = snapshot.sessions.first

        VStack(alignment: .leading, spacing: 1) {
            HStack(spacing: 4) {
                Text(snapshot.summary)
                Spacer(minLength: 2)
                if let asOf {
                    Text(asOf.stamp)
                } else if let top {
                    SessionTime(session: top, asOf: nil)
                }
            }
            .font(Face.sans(13))
            .lineLimit(1)
            .widgetAccentable()

            if let top {
                Text(top.title)
                    .font(Face.sans(13))
                    .lineLimit(1)
                    .privacySensitive()
                Text(top.detail)
                    .font(Face.sans(11))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .privacySensitive()
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .widgetURL(top.map { ZeronLink.chat($0.id) } ?? ZeronLink.sessions)
    }
}

/// the count that matters most right now: what's waiting on the user, else
/// what's running. a stale snapshot trades the secondary count for when it
/// was true.
private struct Headline: View {
    let snapshot: WidgetSnapshot
    let asOf: Date?

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            if snapshot.needsYou > 0 {
                Text("\(snapshot.needsYou)")
                    .font(Face.mono(26))
                    .foregroundStyle(Tone.warning)
                Text(caption("Need you", extra: snapshot.working > 0 ? "\(snapshot.working) working" : nil))
                    .font(Face.sans(12))
                    .foregroundStyle(.secondary)
            } else if snapshot.working > 0 {
                Text("\(snapshot.working)")
                    .font(Face.mono(26))
                    .foregroundStyle(Tone.accent)
                Text(caption("Working"))
                    .font(Face.sans(12))
                    .foregroundStyle(.secondary)
            } else {
                Text("All clear")
                    .font(Face.sans(17))
                Text(caption(snapshot.done > 0 ? "\(snapshot.done) finished" : "Nothing running"))
                    .font(Face.sans(12))
                    .foregroundStyle(.secondary)
            }
        }
        .lineLimit(1)
    }

    private func caption(_ label: String, extra: String? = nil) -> String {
        let tail = asOf.map { "as of \($0.stamp)" } ?? extra
        return [label, tail].compactMap { $0 }.joined(separator: " · ")
    }
}

#if DEBUG
#Preview("Small", as: .systemSmall) {
    SessionsWidget()
} timeline: {
    SnapshotEntry(date: .now, snapshot: .sample)
    SnapshotEntry(date: .now.addingTimeInterval(WidgetSnapshot.staleAfter), snapshot: .sample)
    SnapshotEntry(date: .now, snapshot: .empty)
    SnapshotEntry(date: .now, snapshot: nil)
}

#Preview("Medium", as: .systemMedium) {
    SessionsWidget()
} timeline: {
    SnapshotEntry(date: .now, snapshot: .sample)
}

#Preview("Rectangular", as: .accessoryRectangular) {
    SessionsWidget()
} timeline: {
    SnapshotEntry(date: .now, snapshot: .sample)
}
#endif
