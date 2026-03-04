/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Hardcodet.Wpf.TaskbarNotification;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Manages the system tray icon and minimize-to-tray behavior.
/// </summary>
public class SystemTrayService : IDisposable
{
    private TaskbarIcon? _trayIcon;
    private readonly Window _mainWindow;
    private readonly CollectionBackgroundService? _backgroundService;
    private bool _disposed;
    private MenuItem? _pauseResumeItem;

    public SystemTrayService(Window mainWindow, CollectionBackgroundService? backgroundService = null)
    {
        _mainWindow = mainWindow;
        _backgroundService = backgroundService;
        Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
    }

    /// <summary>
    /// Initializes the system tray icon with context menu.
    /// </summary>
    public void Initialize()
    {
        _trayIcon?.Dispose();

        _trayIcon = new TaskbarIcon();

        /* Use plain string tooltip to avoid Hardcodet TrayToolTip crash (issue #422).
           Custom visual tooltips trigger a race condition in Popup.CreateWindow
           that throws "The root Visual of a VisualTarget cannot have a parent." */
        _trayIcon.ToolTipText = "Performance Monitor Lite";

        /* Load icon */
        try
        {
            var iconUri = new Uri("pack://application:,,,/EDD.ico", UriKind.Absolute);
            _trayIcon.IconSource = new BitmapImage(iconUri);
        }
        catch
        {
            /* Icon loading failed - tray icon will be blank but functional */
        }

        /* Build context menu */
        var contextMenu = new ContextMenu();

        var showItem = new MenuItem { Header = "Show Window", Icon = new TextBlock { Text = "📊", Background = Brushes.Transparent } };
        showItem.Click += (s, e) => ShowMainWindow();
        contextMenu.Items.Add(showItem);

        contextMenu.Items.Add(new Separator());

        _pauseResumeItem = new MenuItem { Header = "Pause Collection", Icon = new TextBlock { Text = "⏸", Background = Brushes.Transparent } };
        _pauseResumeItem.Click += (s, e) => ToggleCollection();
        contextMenu.Items.Add(_pauseResumeItem);

        contextMenu.Items.Add(new Separator());

        var exitItem = new MenuItem { Header = "Exit", Icon = new TextBlock { Text = "✕", Background = Brushes.Transparent } };
        exitItem.Click += (s, e) => ExitApplication();
        contextMenu.Items.Add(exitItem);

        _trayIcon.ContextMenu = contextMenu;

        /* Double-click to show window */
        _trayIcon.TrayMouseDoubleClick += (s, e) => ShowMainWindow();

        /* Handle minimize to tray */
        _mainWindow.StateChanged += MainWindow_StateChanged;
    }

    private void MainWindow_StateChanged(object? sender, EventArgs e)
    {
        if (_mainWindow.WindowState == WindowState.Minimized && App.MinimizeToTray)
        {
            _mainWindow.Hide();
        }
    }

    private void ShowMainWindow()
    {
        _mainWindow.Show();
        _mainWindow.ShowInTaskbar = true;
        _mainWindow.WindowState = WindowState.Normal;
        _mainWindow.Activate();
    }

    private void ToggleCollection()
    {
        if (_backgroundService == null) return;

        _backgroundService.IsPaused = !_backgroundService.IsPaused;

        if (_pauseResumeItem != null)
        {
            _pauseResumeItem.Header = _backgroundService.IsPaused ? "Resume Collection" : "Pause Collection";
            _pauseResumeItem.Icon = new TextBlock { Text = _backgroundService.IsPaused ? "▶" : "⏸", Background = Brushes.Transparent };
        }

        if (_trayIcon != null)
        {
            _trayIcon.ToolTipText = _backgroundService.IsPaused
                ? "Performance Monitor Lite (Paused)"
                : "Performance Monitor Lite";
        }
    }

    private void ExitApplication()
    {
        Application.Current.Shutdown();
    }

    /// <summary>
    /// Shows a balloon notification from the system tray icon.
    /// </summary>
    public void ShowNotification(string title, string message, BalloonIcon icon)
    {
        _trayIcon?.ShowBalloonTip(title, message, icon);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing && _trayIcon != null)
        {
            Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
            _mainWindow.StateChanged -= MainWindow_StateChanged;
            _trayIcon.Visibility = Visibility.Collapsed;
            _trayIcon.Dispose();
            _trayIcon = null;
        }

        _disposed = true;
    }

    private void OnThemeChanged(string _)
    {
        _mainWindow.Dispatcher.InvokeAsync(Initialize);
    }
}

